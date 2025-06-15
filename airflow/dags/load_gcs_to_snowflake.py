"""
Load weather data from GCS to Snowflake

Loads weather data from GCS partitioned files into Snowflake data warehouse

COPY approaches for duplicate handling:

1. load_weather_data_to_snowflake(): 
   - File-level duplicate prevention only (Snowflake's built-in COPY behavior)
   - Same file won't be loaded twice, but if file content changes, all records load again

2. load_weather_data_upsert():
   - File-level + Record-level duplicate prevention  
   - Uses staging table + MERGE on job_id to update existing records or insert new ones
   - Handles both file changes and individual record updates intelligently
"""  

import logging
import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
import sys

sys.path.append('/opt/airflow/project')
from infra.connections_snowflake import SnowflakeConnector

# Configuration
GCS_BUCKET_NAME = os.getenv('GCS_BUCKET_NAME')
GCS_FOLDER_NAME = os.getenv('GCS_FOLDER_NAME')

SNOWFLAKE_ACCOUNT = os.getenv('SNOWFLAKE_ACCOUNT')
SNOWFLAKE_USER = os.getenv('SNOWFLAKE_USER')
SNOWFLAKE_PASSWORD = os.getenv('SNOWFLAKE_PASSWORD')
SNOWFLAKE_WAREHOUSE = os.getenv('SNOWFLAKE_WAREHOUSE')
SNOWFLAKE_DATABASE = os.getenv('SNOWFLAKE_DATABASE')
SNOWFLAKE_SCHEMA = os.getenv('SNOWFLAKE_SCHEMA')

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
    'load_gcs_to_snowflake',
    default_args=default_args,
    description='Load weather data from GCS to Snowflake',
    schedule_interval='30 */6 * * *',  # Every 6 hours
    catchup=False,
    max_active_runs=1,
    tags=['etl', 'snowflake', 'gcs', 'data-warehouse'],
)

# Schema configuration
EXPECTED_WEATHER_SCHEMA = {
    'job_id': {'type': 'STRING', 'nullable': False},
    'location_name': {'type': 'STRING', 'nullable': True},
    'latitude': {'type': 'FLOAT', 'nullable': True},
    'longitude': {'type': 'FLOAT', 'nullable': True},
    'processed_at': {'type': 'TIMESTAMP', 'nullable': True},
    'coord_lon': {'type': 'FLOAT', 'nullable': True},
    'coord_lat': {'type': 'FLOAT', 'nullable': True},
    'weather_id': {'type': 'INTEGER', 'nullable': True},
    'weather_main': {'type': 'STRING', 'nullable': True},
    'weather_description': {'type': 'STRING', 'nullable': True},
    'weather_icon': {'type': 'STRING', 'nullable': True},
    'temperature': {'type': 'FLOAT', 'nullable': True},
    'feels_like': {'type': 'FLOAT', 'nullable': True},
    'temp_min': {'type': 'FLOAT', 'nullable': True},
    'temp_max': {'type': 'FLOAT', 'nullable': True},
    'pressure': {'type': 'INTEGER', 'nullable': True},
    'humidity': {'type': 'INTEGER', 'nullable': True},
    'sea_level': {'type': 'INTEGER', 'nullable': True},
    'grnd_level': {'type': 'INTEGER', 'nullable': True},
    'wind_speed': {'type': 'FLOAT', 'nullable': True},
    'wind_deg': {'type': 'INTEGER', 'nullable': True},
    'wind_gust': {'type': 'FLOAT', 'nullable': True},
    'visibility': {'type': 'INTEGER', 'nullable': True},
    'clouds_all': {'type': 'INTEGER', 'nullable': True},
    'dt': {'type': 'INTEGER', 'nullable': True},
    'api_timestamp': {'type': 'TIMESTAMP', 'nullable': True},
    'timezone': {'type': 'INTEGER', 'nullable': True},
    'city_id': {'type': 'INTEGER', 'nullable': True},
    'city_name': {'type': 'STRING', 'nullable': True},
    'country': {'type': 'STRING', 'nullable': True},
    'sunrise': {'type': 'TIMESTAMP', 'nullable': True},
    'sunset': {'type': 'TIMESTAMP', 'nullable': True},
    'sys_type': {'type': 'INTEGER', 'nullable': True},
    'sys_id': {'type': 'INTEGER', 'nullable': True},
    'base': {'type': 'STRING', 'nullable': True},
    'cod': {'type': 'INTEGER', 'nullable': True},
    'raw_response': {'type': 'VARIANT', 'nullable': True},
}


# Main functions #

def setup_snowflake_infrastructure(**context):
    """Setup Snowflake database, schema, integration, and table with safe schema evolution"""
    
    snowflake_conn = SnowflakeConnector(
        SNOWFLAKE_ACCOUNT, SNOWFLAKE_USER, SNOWFLAKE_PASSWORD, 
        SNOWFLAKE_WAREHOUSE
    )
    
    try:
        # Setup database and schema
        setup_queries = [
            "USE ROLE ACCOUNTADMIN",
            "CREATE DATABASE IF NOT EXISTS WEATHER_DB",
            "USE DATABASE WEATHER_DB",
            "CREATE SCHEMA IF NOT EXISTS RAW_DATA",
            "USE SCHEMA RAW_DATA",
        ]

        for query in setup_queries:
            snowflake_conn.execute_query(query)

        # Setup GCS integration
        snowflake_conn.setup_gcs_integration(GCS_BUCKET_NAME, GCS_FOLDER_NAME)

        # Safe table creation/evolution
        snowflake_conn.setup_table_with_schema('WEATHER_DATA', EXPECTED_WEATHER_SCHEMA)
            
        logging.info("Snowflake infrastructure setup completed")
        
    finally:
        snowflake_conn.close()
        
      
def load_weather_data_to_snowflake(**context):
    """Load weather data from GCS to Snowflake for execution date"""
    
    exec_date = context['ds']
    year, month, day = exec_date.split('-')
    
    snowflake_conn = SnowflakeConnector(
        SNOWFLAKE_ACCOUNT, SNOWFLAKE_USER, SNOWFLAKE_PASSWORD, 
        SNOWFLAKE_WAREHOUSE
    )
    
    try:
        # Set context
        snowflake_conn.execute_query("USE ROLE ACCOUNTADMIN")
        snowflake_conn.execute_query("USE DATABASE WEATHER_DB")
        snowflake_conn.execute_query("USE SCHEMA RAW_DATA")
        snowflake_conn.execute_query(f"USE WAREHOUSE {SNOWFLAKE_WAREHOUSE}")
        
        # Verify integration exists
        try:
            check_integration = "SHOW INTEGRATIONS LIKE 'GCS_INT'"
            integration_result = snowflake_conn.execute_query(check_integration)
            if not integration_result:
                logging.warning("GCS_INT integration not found. Recreating...")
                snowflake_conn.setup_gcs_integration(GCS_BUCKET_NAME, GCS_FOLDER_NAME)
            else:
                logging.info("GCS_INT integration found")
        except Exception as integration_error:
            logging.warning(f"Integration check failed: {integration_error}. Recreating...")
            snowflake_conn.setup_gcs_integration(GCS_BUCKET_NAME, GCS_FOLDER_NAME)
                
        # Build the GCS path
        gcs_path = f"gcs://{GCS_BUCKET_NAME}/{GCS_FOLDER_NAME}/year={year}/month={month}/day={day}/"
        logging.info(f"Loading from: {gcs_path}")
        
        # Load data using COPY command with direct GCS path
        copy_query = f"""
        COPY INTO WEATHER_DATA
        FROM '{gcs_path}'
        STORAGE_INTEGRATION = GCS_INT
        FILE_FORMAT = (TYPE = PARQUET)
        MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
        ON_ERROR = CONTINUE
        RETURN_FAILED_ONLY = TRUE
        """
        
        copy_result = snowflake_conn.execute_query(copy_query)
        logging.info(f"Copy result: {copy_result}")
                
    except Exception as e:
        logging.error(f"Load failed: {e}")
        raise
        
    finally:
        snowflake_conn.close()
        
        
def load_weather_data_upsert(**context):
    """
    UPSERT LOAD: Update existing records or insert new ones based on job_id
    Use this when you want to overwrite/update existing records with new data
    """
    
    exec_date = context['ds']
    year, month, day = exec_date.split('-')
    
    snowflake_conn = SnowflakeConnector(
        SNOWFLAKE_ACCOUNT, SNOWFLAKE_USER, SNOWFLAKE_PASSWORD, 
        SNOWFLAKE_WAREHOUSE
    )
    
    try:
        # Set context
        snowflake_conn.execute_query("USE ROLE ACCOUNTADMIN")
        snowflake_conn.execute_query("USE DATABASE WEATHER_DB")
        snowflake_conn.execute_query("USE SCHEMA RAW_DATA")
        snowflake_conn.execute_query(f"USE WAREHOUSE {SNOWFLAKE_WAREHOUSE}")
        
        # Verify integration exists
        try:
            check_integration = "SHOW INTEGRATIONS LIKE 'GCS_INT'"
            integration_result = snowflake_conn.execute_query(check_integration)
            if not integration_result:
                logging.warning("GCS_INT integration not found. Recreating...")
                snowflake_conn.setup_gcs_integration(GCS_BUCKET_NAME, GCS_FOLDER_NAME)
        except Exception as integration_error:
            logging.warning(f"Integration check failed: {integration_error}. Recreating...")
            snowflake_conn.setup_gcs_integration(GCS_BUCKET_NAME, GCS_FOLDER_NAME)
        
        # Create staging table
        create_staging_query = """
        CREATE OR REPLACE TEMPORARY TABLE WEATHER_DATA_STAGING LIKE WEATHER_DATA
        """
        snowflake_conn.execute_query(create_staging_query)
        
        # Build the GCS path and load into staging
        gcs_path = f"gcs://{GCS_BUCKET_NAME}/{GCS_FOLDER_NAME}/year={year}/month={month}/day={day}/"
        logging.info(f"Upsert loading from: {gcs_path}")
        
        copy_query = f"""
        COPY INTO WEATHER_DATA_STAGING
        FROM '{gcs_path}'
        STORAGE_INTEGRATION = GCS_INT
        FILE_FORMAT = (TYPE = PARQUET)
        MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
        ON_ERROR = CONTINUE
        RETURN_FAILED_ONLY = TRUE
        """
        
        copy_result = snowflake_conn.execute_query(copy_query)
        logging.info(f"Copy result for temporary staging table : {copy_result}")
        # Check staging row count
        staging_count_query = "SELECT COUNT(*) FROM WEATHER_DATA_STAGING"
        staging_result = snowflake_conn.execute_query(staging_count_query)
        staging_rows = staging_result[0][0]
        
        if staging_rows == 0:
            logging.info(f"No new data to upsert for {exec_date}")
            return {'status': 'skipped', 'reason': 'no_data', 'mode': 'upsert'}
        
        logging.info(f"Loaded {staging_rows} rows into staging for upsert")
        
        # Get count before MERGE
        before_count_query = "SELECT COUNT(*) FROM WEATHER_DATA"
        before_result = snowflake_conn.execute_query(before_count_query)
        before_count = before_result[0][0]
        
        # Execute MERGE (upsert)
        merge_query = """
        MERGE INTO WEATHER_DATA AS target
        USING WEATHER_DATA_STAGING AS source
        ON target.job_id = source.job_id
        WHEN MATCHED THEN 
            UPDATE SET 
                location_name = source.location_name,
                latitude = source.latitude,
                longitude = source.longitude,
                processed_at = source.processed_at,
                coord_lon = source.coord_lon,
                coord_lat = source.coord_lat,
                weather_id = source.weather_id,
                weather_main = source.weather_main,
                weather_description = source.weather_description,
                weather_icon = source.weather_icon,
                temperature = source.temperature,
                feels_like = source.feels_like,
                temp_min = source.temp_min,
                temp_max = source.temp_max,
                pressure = source.pressure,
                humidity = source.humidity,
                sea_level = source.sea_level,
                grnd_level = source.grnd_level,
                wind_speed = source.wind_speed,
                wind_deg = source.wind_deg,
                wind_gust = source.wind_gust,
                visibility = source.visibility,
                clouds_all = source.clouds_all,
                dt = source.dt,
                api_timestamp = source.api_timestamp,
                timezone = source.timezone,
                city_id = source.city_id,
                city_name = source.city_name,
                country = source.country,
                sunrise = source.sunrise,
                sunset = source.sunset,
                sys_type = source.sys_type,
                sys_id = source.sys_id,
                base = source.base,
                cod = source.cod,
                raw_response = source.raw_response,
        WHEN NOT MATCHED THEN 
            INSERT (job_id, location_name, latitude, longitude, processed_at, coord_lon, coord_lat, 
                   weather_id, weather_main, weather_description, weather_icon, temperature, feels_like,
                   temp_min, temp_max, pressure, humidity, sea_level, grnd_level, wind_speed, wind_deg,
                   wind_gust, visibility, clouds_all, dt, api_timestamp, timezone, city_id, city_name,
                   country, sunrise, sunset, sys_type, sys_id, base, cod, raw_response)
            VALUES (source.job_id, source.location_name, source.latitude, source.longitude, 
                   source.processed_at, source.coord_lon, source.coord_lat, source.weather_id,
                   source.weather_main, source.weather_description, source.weather_icon, 
                   source.temperature, source.feels_like, source.temp_min, source.temp_max,
                   source.pressure, source.humidity, source.sea_level, source.grnd_level,
                   source.wind_speed, source.wind_deg, source.wind_gust, source.visibility,
                   source.clouds_all, source.dt, source.api_timestamp, source.timezone,
                   source.city_id, source.city_name, source.country, source.sunrise, source.sunset,
                   source.sys_type, source.sys_id, source.base, source.cod, source.raw_response)
        """
        
        merge_result = snowflake_conn.execute_query(merge_query)
        logging.info(f"Merge result: {merge_result}")
        
        # Get count after MERGE
        after_count_query = "SELECT COUNT(*) FROM WEATHER_DATA"
        after_result = snowflake_conn.execute_query(after_count_query)
        after_count = after_result[0][0]
        
        # Calculate stats
        net_new_records = after_count - before_count
        updated_records = staging_rows - net_new_records
        
        logging.info(f"Upsert completed: {net_new_records} new records, {updated_records} updated records")
        
        return {
            'status': 'success', 
            'mode': 'upsert',
            'date': exec_date, 
            'staging_rows': staging_rows,
            'new_records': net_new_records,
            'updated_records': updated_records,
            'total_records': after_count
        }
        
    except Exception as e:
        logging.error(f"Upsert load failed: {e}")
        raise
        
    finally:
        snowflake_conn.close()
        
                
# Tasks
setup_snowflake_infrastructure_task = PythonOperator(
    task_id='setup_snowflake_infrastructure',
    python_callable=setup_snowflake_infrastructure,
    dag=dag,
)

load_weather_data_to_snowflake_task = PythonOperator(
    task_id='load_weather_data_to_snowflake',
    python_callable=load_weather_data_to_snowflake,
    dag=dag,
)

# Dependencies
setup_snowflake_infrastructure_task >> load_weather_data_to_snowflake_task