"""
Transfer weather data from MongoDB to GCS

1. Upload data to GCS and track successful uploads
2. Delete uploaded records from MongoDB
"""

import logging
import os
import pandas as pd
from datetime import datetime, timedelta
from typing import Set
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
import pyarrow as pa
import pyarrow.parquet as pq

import sys
sys.path.append('/opt/airflow/project')
from infra.connections_redis import RedisQueue
from infra.connections_mongodb import MongoDBJobDB
from infra.connections_gcs import GCSConnector

# Configuration
REDIS_HOST = os.getenv('REDIS_HOST')
REDIS_PORT = int(os.getenv('REDIS_PORT'))
REDIS_DB = int(os.getenv('REDIS_DB'))
UPLOAD_QUEUE_NAME = 'cloud_upload_queue'
DELETION_QUEUE_NAME = 'deletion_queue'
FAILED_UPLOAD_QUEUE = 'failed_upload_queue'

MONGODB_CONNECTION = os.getenv('MONGODB_CONNECTION_STRING')
MONGODB_DATABASE = os.getenv('MONGODB_DATABASE')
MONGODB_COLLECTION = os.getenv('MONGODB_COLLECTION')

GCP_PROJECT_ID = os.getenv('GCP_PROJECT_ID')
GCS_BUCKET_NAME = os.getenv('GCS_BUCKET_NAME')
GCS_FOLDER_NAME = os.getenv('GCS_FOLDER_NAME')
GCP_CREDENTIALS_PATH = os.getenv('GCP_CREDENTIALS_PATH')

TEMP_DIR = '/tmp/weather_data'

# Default arguments
default_args = {
    'owner': 'data-engineering',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

# DAG definition
dag = DAG(
    'transfer_mongodb_to_gcs',
    default_args=default_args,
    description='Transfer weather data from MongoDB to GCS and cleanup',
    schedule_interval='15 */3 * * *',  # Run every 3 hours
    catchup=False,
    max_active_runs=1,
    tags=['weather', 'gcs', 'transfer', 'cleanup'],
)

# Helper/Utility functions
def process_mongodb_batch(records):
    """
    Post-process MongoDB records before converting to Parquet
    any data transformations, cleaning, or enrichment here
    """
    if not records:
        return records
    
    # Convert to DataFrame for easier processing
    df = pd.DataFrame(records)
    
    # Remove MongoDB _id field and worker_id
    columns_to_drop = ['_id', 'worker_id']
    existing_columns_to_drop = [col for col in columns_to_drop if col in df.columns]
    if existing_columns_to_drop:
        df = df.drop(existing_columns_to_drop, axis=1)
    
    # Convert datetime columns
    datetime_columns = ['processed_at', 'api_timestamp', 'sunrise', 'sunset']
    for col in datetime_columns:
        if col in df.columns:
            # Convert to datetime first if needed, then to ISO string
            if df[col].dtype == 'object':
                df[col] = pd.to_datetime(df[col], errors='coerce')
            
            # Convert to ISO format string (YYYY-MM-DD HH:MM:SS.sss)
            df[col] = df[col].dt.strftime('%Y-%m-%d %H:%M:%S.%f').str[:-3]  # Remove last 3 digits for milliseconds
            
            # Handle NaT/null values
            df[col] = df[col].replace('NaT', None)
    
    # Any future additional processing here:
    # - Data validation
    # - Field transformations
    # - Calculated fields
    # - Data cleaning
    
    return df.to_dict('records')


# Main processing function

def upload_to_gcs(**context):
    """Process job IDs from queue, create Parquet file, upload to GCS"""
    
    redis_queue = RedisQueue(REDIS_HOST, REDIS_PORT, REDIS_DB)
    mongo_db = MongoDBJobDB(MONGODB_CONNECTION, MONGODB_DATABASE)
    gcs_connector = GCSConnector(GCP_PROJECT_ID, GCP_CREDENTIALS_PATH)
    
    processed_count = 0
    failed_job_ids: Set[str] = set()
    batch_job_ids = []
    
    try:
        os.makedirs(TEMP_DIR, exist_ok=True)
        
        # Create filename with timestamp
        exec_time = context['execution_date']
        if hasattr(exec_time, 'datetime'):
            exec_time = exec_time.datetime
        
        timestamp = exec_time.strftime('%Y%m%d_%H%M%S')
        filename = f"weather_data_{timestamp}.parquet"
        local_path = os.path.join(TEMP_DIR, filename)
        
        parquet_writer = None
        
        # Process all batches
        while True:
            job_ids = redis_queue.batch_lpop(UPLOAD_QUEUE_NAME, 100)
            if not job_ids:
                break
            
            batch_job_ids.extend(job_ids)
            logging.info(f"Processing batch of {len(job_ids)} job IDs")
            
            try:
                # Fetch data from MongoDB
                query = {'job_id': {'$in': job_ids}}
                cursor = mongo_db.get_documents_batch(MONGODB_COLLECTION, query)
                records = list(cursor)
                
                if not records:
                    logging.warning(f"No records found for job IDs: {job_ids}")
                    failed_job_ids.update(job_ids)
                    continue
                
                # Track missing job IDs
                found_job_ids = {record['job_id'] for record in records}
                missing_job_ids = set(job_ids) - found_job_ids
                
                if missing_job_ids:
                    logging.warning(f"Missing job IDs in MongoDB: {missing_job_ids}")
                    failed_job_ids.update(missing_job_ids)
                
                # Post-process the batch
                processed_records = process_mongodb_batch(records)
                df_batch = pd.DataFrame(processed_records)
                
                table = pa.Table.from_pandas(df_batch)
                
                # Initialize or append to Parquet file
                if parquet_writer is None:
                    parquet_writer = pq.ParquetWriter(local_path, table.schema)
                
                parquet_writer.write_table(table)
                processed_count += len(records)
                
                logging.info(f"Appended {len(records)} records, total: {processed_count}")
                
            except Exception as batch_error:
                logging.error(f"Error processing batch {job_ids}: {batch_error}")
                failed_job_ids.update(job_ids)
                continue
        
        # Close writer
        if parquet_writer is not None:
            parquet_writer.close()
        
        # Upload if we have data
        if processed_count > 0:
            try:
                # Create GCS path with date partitioning
                year = exec_time.year
                month = exec_time.month
                day = exec_time.day
                gcs_path = f"{GCS_FOLDER_NAME}/year={year}/month={month:02d}/day={day:02d}/{filename}"
                
                # Upload to GCS
                gcs_url = gcs_connector.upload_file(GCS_BUCKET_NAME, local_path, gcs_path)
                
                # Clean up local file
                os.remove(local_path)
                
                logging.info(f"Upload successful: {processed_count} records to {gcs_url}")
                
                # Queue successful job IDs for deletion
                successful_job_ids = set(batch_job_ids) - failed_job_ids
                if successful_job_ids:
                    redis_queue.batch_rpush(DELETION_QUEUE_NAME, list(successful_job_ids))
                    logging.info(f"Added {len(successful_job_ids)} job IDs to deletion queue")
                
                # Queue failed job IDs
                if failed_job_ids:
                    redis_queue.batch_rpush(FAILED_UPLOAD_QUEUE, list(failed_job_ids))
                    logging.info(f"Added {len(failed_job_ids)} job IDs to failed upload queue")
                
                return {
                    'processed_records': processed_count,
                    'uploaded_files': [gcs_url],
                    'failed_job_ids': len(failed_job_ids)
                }
                
            except Exception as upload_error:
                logging.error(f"GCS upload failed: {upload_error}")
                
                # Restore job IDs to queue on upload failure
                if batch_job_ids:
                    redis_queue.batch_rpush(UPLOAD_QUEUE_NAME, batch_job_ids)
                    logging.info(f"Restored {len(batch_job_ids)} job IDs to upload queue")
                
                raise upload_error
        else:
            logging.info("No data to upload")
            
            # Queue failed job IDs even if no data
            if failed_job_ids:
                redis_queue.batch_rpush(FAILED_UPLOAD_QUEUE, list(failed_job_ids))
                logging.info(f"Added {len(failed_job_ids)} job IDs to failed upload queue")
            
            return {
                'processed_records': 0,
                'uploaded_files': [],
                'failed_job_ids': len(failed_job_ids)
            }
    
    except Exception as e:
        logging.error(f"Upload task failed: {e}")
        
        # Restore job IDs on task failure
        if batch_job_ids:
            redis_queue.batch_rpush(UPLOAD_QUEUE_NAME, batch_job_ids)
            logging.error(f"Restored {len(batch_job_ids)} job IDs to upload queue")
        
        # Close writer on error
        if parquet_writer is not None:
            parquet_writer.close()
        raise
    
    finally:
        redis_queue.close()
        mongo_db.close()
        gcs_connector.close()


def delete_uploaded_records(**context):
    """Delete records that were successfully uploaded to GCS"""
    
    redis_queue = RedisQueue(REDIS_HOST, REDIS_PORT, REDIS_DB)
    mongo_db = MongoDBJobDB(MONGODB_CONNECTION, MONGODB_DATABASE)
    
    total_deleted = 0
    batches_processed = 0
    
    try:
        logging.info("Starting deletion of uploaded records")
        
        # Process deletion queue in batches
        while True:
            job_ids = redis_queue.batch_lpop(DELETION_QUEUE_NAME, 100)
            if not job_ids:
                break
            
            try:
                # Delete records
                delete_result = mongo_db.batch_delete_by_query(
                    MONGODB_COLLECTION,
                    {'job_id': {'$in': job_ids}}
                )
                
                batch_deleted = getattr(delete_result, 'deleted_count', 0)
                total_deleted += batch_deleted
                batches_processed += 1
                
                logging.info(f"Batch {batches_processed}: deleted {batch_deleted} records")
                
            except Exception as batch_error:
                logging.error(f"Error deleting batch {job_ids}: {batch_error}")
                continue
        
        logging.info(f"Deletion completed: {total_deleted} records in {batches_processed} batches")
        
        return {
            "deleted_count": total_deleted,
            "batches_processed": batches_processed
        }
        
    except Exception as e:
        logging.error(f"Deletion task failed: {e}")
        raise
        
    finally:
        redis_queue.close()
        mongo_db.close()


# Tasks
upload_task = PythonOperator(
    task_id='upload_to_gcs',
    python_callable=upload_to_gcs,
    dag=dag,
)

cleanup_task = PythonOperator(
    task_id='delete_uploaded_records',
    python_callable=delete_uploaded_records,
    dag=dag,
)

# Dependencies
upload_task >> cleanup_task