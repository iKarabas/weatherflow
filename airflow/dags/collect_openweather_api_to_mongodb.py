"""
OpenWeather API to MongoDB ETL Pipeline

Collects weather data from OpenWeather API and stores in MongoDB using Redis queue
"""

import logging
import os
import pandas as pd
import requests
from datetime import datetime, timedelta
import time
from concurrent.futures import ThreadPoolExecutor
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
import sys

sys.path.append("/opt/airflow/project")
from infra.connections_redis import RedisQueue
from infra.connections_mongodb import MongoDBJobDB

# Configuration
REDIS_HOST = os.getenv("REDIS_HOST")
REDIS_PORT = int(os.getenv("REDIS_PORT"))
REDIS_DB = int(os.getenv("REDIS_DB"))
WEATHER_API_QUEUE_NAME = "weather_api_requests_metadata_queue"
CLOUD_UPLOAD_QUEUE_NAME = "cloud_upload_queue"

MONGODB_CONNECTION = os.getenv("MONGODB_CONNECTION_STRING")
MONGODB_DATABASE = os.getenv("MONGODB_DATABASE")
MONGODB_COLLECTION = os.getenv("MONGODB_COLLECTION")

OPENWEATHER_API_KEY = os.getenv("OPENWEATHER_API_KEY")
CSV_FILE = "/opt/airflow/project/data/de_top30_cities_plus_selected_international.csv"
WORKER_COUNT = 2

# Default arguments
default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "start_date": days_ago(1),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

# DAG definition
dag = DAG(
    "collect_openweather_api_to_mongodb",
    default_args=default_args,
    description="Collect weather data from OpenWeather API and store in MongoDB",
    schedule_interval="0 * * * *", # minute hour day_of_month month day_of_week
    catchup=False,
    max_active_runs=1,
    tags=["etl", "openweather-api", "mongodb", "data-collection"],
)

# Helper/Utility functions #

def process_weather_response(weather_data, job, worker_id):
    """Helper function to process OpenWeather API response into structured record"""

    # Helper to safely get nested values
    coord = weather_data.get("coord", {})
    weather = weather_data.get("weather", [{}])[0] if weather_data.get("weather") else {}
    main = weather_data.get("main", {})
    wind = weather_data.get("wind", {})
    clouds = weather_data.get("clouds", {})
    sys = weather_data.get("sys", {})

    return {
        # Job metadata
        "job_id": job.get("job_id"),
        "location_name": job.get("location_name"),
        "latitude": job.get("lat"),
        "longitude": job.get("lon"),
        "worker_id": worker_id,
        "processed_at": datetime.utcnow(),
        # Coordinates
        "coord_lon": coord.get("lon"),
        "coord_lat": coord.get("lat"),
        # Weather info
        "weather_id": weather.get("id"),
        "weather_main": weather.get("main"),
        "weather_description": weather.get("description"),
        "weather_icon": weather.get("icon"),
        # Temperature data
        "temperature": main.get("temp"),
        "feels_like": main.get("feels_like"),
        "temp_min": main.get("temp_min"),
        "temp_max": main.get("temp_max"),
        "pressure": main.get("pressure"),
        "humidity": main.get("humidity"),
        "sea_level": main.get("sea_level"),
        "grnd_level": main.get("grnd_level"),
        # Wind data
        "wind_speed": wind.get("speed"),
        "wind_deg": wind.get("deg"),
        "wind_gust": wind.get("gust"),
        # Other conditions
        "visibility": weather_data.get("visibility"),
        "clouds_all": clouds.get("all"),
        # System data
        "dt": weather_data.get("dt"),
        "api_timestamp": datetime.fromtimestamp(weather_data.get("dt")) if weather_data.get("dt") else None,
        "timezone": weather_data.get("timezone"),
        "city_id": weather_data.get("id"),
        "city_name": weather_data.get("name"),
        "country": sys.get("country"),
        "sunrise": datetime.fromtimestamp(sys.get("sunrise")) if sys.get("sunrise") else None,
        "sunset": datetime.fromtimestamp(sys.get("sunset")) if sys.get("sunset") else None,
        "sys_type": sys.get("type"),
        "sys_id": sys.get("id"),
        # API metadata
        "base": weather_data.get("base"),
        "cod": weather_data.get("cod"),
        # Raw response, in case needed for debugging or future use
        "raw_response": weather_data,
    }


def make_request_with_retry(url, params, timeout=30, retries=3):
    """Simple request with retry logic"""
    import time

    for attempt in range(retries):
        try:
            response = requests.get(url, params=params, timeout=timeout)
            response.raise_for_status()
            return response.json()
        except Exception as e:
            if attempt == retries - 1:  # Last attempt
                raise e
            time.sleep(2)  # Wait 2 seconds before retry


def worker_process(worker_id):
    redis_queue = RedisQueue(REDIS_HOST, REDIS_PORT, REDIS_DB)
    mongo_db = MongoDBJobDB(MONGODB_CONNECTION, MONGODB_DATABASE)

    processed = 0
    failed = 0
    batch = []
    success_ids = []

    while True:
        jobs = redis_queue.batch_lpop(WEATHER_API_QUEUE_NAME, count=10)
        if not jobs:
            break

        for job in jobs:
            try:
                # Fetch weather data from OpenWeather API
                url = "https://api.openweathermap.org/data/2.5/weather"
                params = {"lat": job["lat"], "lon": job["lon"], "appid": OPENWEATHER_API_KEY, "units": "metric"}

                weather_data = make_request_with_retry(url, params)
                print(weather_data)

                # Process ALL available data fields
                record = process_weather_response(weather_data, job, worker_id)
                batch.append(record)
                processed += 1

                # Save batch and collect successful IDs
                if len(batch) >= 20:
                    mongo_db.upsert_documents(MONGODB_COLLECTION, batch, "job_id")
                    success_ids.extend([r["job_id"] for r in batch])
                    batch.clear()
                    
            except Exception as e:
                logging.error(f"Worker {worker_id} failed job {job['job_id']}: {e}")
                failed += 1

    # Save remaining batch
    if batch:
        mongo_db.upsert_documents(MONGODB_COLLECTION, batch, "job_id")
        success_ids.extend([r["job_id"] for r in batch])

    # Push successful job IDs to cloud upload queue
    if success_ids:
        redis_queue.batch_rpush(CLOUD_UPLOAD_QUEUE_NAME, success_ids)
        logging.info(f"Worker {worker_id} queued {len(success_ids)} IDs for cloud upload")

    redis_queue.close()
    mongo_db.close()

    return {"worker_id": worker_id, "processed": processed, "failed": failed, "success_ids_queued": len(success_ids)}


# Main functions #

def schedule_openweather_api_requests(**context):
    """Schedule OpenWeather API requests for all locations"""
    # Initialize MongoDB connection
    mongo_db = MongoDBJobDB(MONGODB_CONNECTION, MONGODB_DATABASE)
    
    # Create indexes for efficient queries (idempotent operation)
    mongo_db.create_index(MONGODB_COLLECTION, "job_id")
    
    # Read CSV
    df = pd.read_csv(CSV_FILE)

    exec_time = context["execution_date"]
    if hasattr(exec_time, "datetime"):
        exec_time = exec_time.datetime

    # Create jobs
    jobs = []
    for _, row in df.iterrows():
        job = {
            "job_id": f"{exec_time.strftime('%Y%m%d_%H')}_{row['name']}",
            "location_name": row["name"],
            "lat": float(row["lat"]),
            "lon": float(row["lon"]),
            "exec_time": exec_time.isoformat(),
        }
        jobs.append(job)

    # Queue jobs in Redis
    redis_queue = RedisQueue(REDIS_HOST, REDIS_PORT, REDIS_DB)
    redis_queue.batch_rpush(WEATHER_API_QUEUE_NAME, jobs)
    redis_queue.close()
    mongo_db.close()
    # Remove it on deployment, it is just for testing, otherwise queue will be consumed before we have it on grafana panel
    # in a real deployment, queue will be larger and will not be consumed before we have it on grafana panel
    time.sleep(30)  
    logging.info(f"Scheduled {len(jobs)} API requests")
    return {"jobs_queued": len(jobs)}


def execute_openweather_collection_and_storage(**context):
    """Execute OpenWeather API collection and store in MongoDB"""
    # Run workers in parallel
    with ThreadPoolExecutor(max_workers=WORKER_COUNT) as executor:
        futures = [executor.submit(worker_process, i) for i in range(WORKER_COUNT)]
        results = [f.result() for f in futures]

    total_processed = sum(result.get("processed", 0) for result in results)
    total_failed = sum(result.get("failed", 0) for result in results)
    total_success_ids = sum(result.get("success_ids_queued", 0) for result in results)

    logging.info(
        f"Processed {total_processed} jobs, failed {total_failed}, queued {total_success_ids} IDs for cloud upload"
    )

    return {
        "total_processed": total_processed,
        "total_failed": total_failed,
        "total_success_ids_queued": total_success_ids,
        "worker_results": results,
    }


# Tasks
schedule_api_requests_task = PythonOperator(
    task_id="schedule_openweather_api_requests",
    python_callable=schedule_openweather_api_requests,
    dag=dag,
)

execute_collection_and_storage_task = PythonOperator(
    task_id="execute_openweather_collection_and_storage",
    python_callable=execute_openweather_collection_and_storage,
    dag=dag,
)

# Dependencies
schedule_api_requests_task >> execute_collection_and_storage_task
