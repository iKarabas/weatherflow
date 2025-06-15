"""
Daily Weather Analytics DAG - Snowflake Analysis

Basic daily weather insights from collected data
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
SNOWFLAKE_ACCOUNT = os.getenv('SNOWFLAKE_ACCOUNT')
SNOWFLAKE_USER = os.getenv('SNOWFLAKE_USER')
SNOWFLAKE_PASSWORD = os.getenv('SNOWFLAKE_PASSWORD')
SNOWFLAKE_WAREHOUSE = os.getenv('SNOWFLAKE_WAREHOUSE')

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
    'analyze_weather_daily_snowflake',
    default_args=default_args,
    description='Daily weather analytics using Snowflake',
    schedule_interval='0 23 * * *',  # Daily at 11 pm
    catchup=False,
    max_active_runs=1,
    tags=['analytics', 'snowflake', 'weather'],
)


def create_daily_summary(**context):
    """Create a simple daily weather summary"""
    
    exec_date = context['ds']
    
    snowflake_conn = SnowflakeConnector(
        SNOWFLAKE_ACCOUNT, SNOWFLAKE_USER, SNOWFLAKE_PASSWORD, 
        SNOWFLAKE_WAREHOUSE
    )
    
    try:
        snowflake_conn.execute_query("USE DATABASE WEATHER_DB")
        snowflake_conn.execute_query("USE SCHEMA RAW_DATA")
        
        # Simple daily summary query
        summary_query = f"""
        SELECT 
            location_name,
            DATE(api_timestamp) as weather_date,
            AVG(temperature) as avg_temp,
            MIN(temperature) as min_temp,
            MAX(temperature) as max_temp,
            AVG(humidity) as avg_humidity,
            weather_main as weather_condition,
            COUNT(*) as data_points
        FROM WEATHER_DATA
        WHERE DATE(api_timestamp) = '{exec_date}'
        GROUP BY location_name, DATE(api_timestamp), weather_main
        ORDER BY location_name
        """
        
        results = snowflake_conn.execute_query(summary_query)
        
        # Log simple insights
        logging.info(f"📊 Weather Summary for {exec_date}")
        logging.info("-" * 50)
        
        if results:
            for row in results:
                location = row[0]
                avg_temp = round(row[2], 1)
                min_temp = round(row[3], 1) 
                max_temp = round(row[4], 1)
                humidity = round(row[5], 1)
                condition = row[6]
                
                logging.info(f"{location}: {avg_temp}°C avg ({min_temp}-{max_temp}°C), {humidity}% humidity, {condition}")
        else:
            logging.info("No data found for this date")
        
        return {"records_processed": len(results)}
        
    except Exception as e:
        logging.error(f"Daily summary failed: {e}")
        raise
        
    finally:
        snowflake_conn.close()


def generate_insights(**context):
    """Generate basic daily insights"""
    
    exec_date = context['ds']
    
    snowflake_conn = SnowflakeConnector(
        SNOWFLAKE_ACCOUNT, SNOWFLAKE_USER, SNOWFLAKE_PASSWORD, 
        SNOWFLAKE_WAREHOUSE
    )
    
    try:
        snowflake_conn.execute_query("USE DATABASE WEATHER_DB")
        snowflake_conn.execute_query("USE SCHEMA RAW_DATA")
        
        # 1. Hottest and coldest locations
        temp_extremes = f"""
        SELECT 
            location_name,
            AVG(temperature) as avg_temp
        FROM WEATHER_DATA
        WHERE DATE(api_timestamp) = '{exec_date}'
        GROUP BY location_name
        ORDER BY avg_temp DESC
        """
        
        temp_results = snowflake_conn.execute_query(temp_extremes)
        
        # 2. Weather conditions summary
        weather_summary = f"""
        SELECT 
            weather_main,
            COUNT(*) as frequency
        FROM WEATHER_DATA
        WHERE DATE(api_timestamp) = '{exec_date}'
        GROUP BY weather_main
        ORDER BY frequency DESC
        """
        
        weather_results = snowflake_conn.execute_query(weather_summary)
        
        # 3. Data quality check
        quality_check = f"""
        SELECT 
            COUNT(*) as total_records,
            COUNT(DISTINCT location_name) as locations,
            AVG(temperature) as global_avg_temp
        FROM WEATHER_DATA
        WHERE DATE(api_timestamp) = '{exec_date}'
        """
        
        quality_results = snowflake_conn.execute_query(quality_check)
        
        # Log insights
        logging.info(f"🌡️ Daily Weather Insights - {exec_date}")
        logging.info("=" * 40)
        
        if temp_results:
            hottest = temp_results[0]
            coldest = temp_results[-1]
            logging.info(f"🔥 Hottest: {hottest[0]} ({round(hottest[1], 1)}°C)")
            logging.info(f"❄️ Coldest: {coldest[0]} ({round(coldest[1], 1)}°C)")
        
        if weather_results:
            logging.info(f"☁️ Weather Conditions:")
            for condition, count in weather_results:
                logging.info(f"   {condition}: {count} readings")
        
        if quality_results:
            total, locations, avg_temp = quality_results[0]
            logging.info(f"📋 Data Summary:")
            logging.info(f"   Total readings: {total}")
            logging.info(f"   Locations: {locations}")
            logging.info(f"   Global average: {round(avg_temp, 1)}°C")
        
        return {
            "total_locations": len(temp_results) if temp_results else 0,
            "weather_conditions": len(weather_results) if weather_results else 0
        }
        
    except Exception as e:
        logging.error(f"Insights generation failed: {e}")
        raise
        
    finally:
        snowflake_conn.close()


# Tasks
summary_task = PythonOperator(
    task_id='create_daily_summary',
    python_callable=create_daily_summary,
    dag=dag,
)

insights_task = PythonOperator(
    task_id='generate_insights',
    python_callable=generate_insights,
    dag=dag,
)

# Dependencies
summary_task >> insights_task