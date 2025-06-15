import os
import time
from prometheus_client import start_http_server, Gauge, CollectorRegistry
import sys

sys.path.append("/app")
from infra.connections_redis import RedisQueue
from infra.connections_mongodb import MongoDBJobDB
from infra.connections_snowflake import SnowflakeConnector


class WeatherMetricsExporter:
    def __init__(self):
        self.metrics = {}
        self.registry = CollectorRegistry()
        
        # Initialize Redis connection
        self.redis_host = os.environ.get('REDIS_HOST')
        self.redis_port = int(os.environ.get('REDIS_PORT'))
        self.redis_db = int(os.environ.get('REDIS_DB'))
        self.redis_queue = RedisQueue(self.redis_host, self.redis_port, self.redis_db)
        
        # Initialize MongoDB connection
        self.mongodb_connection_string = os.environ.get("MONGODB_CONNECTION_STRING")
        self.mongodb_database = os.environ.get("MONGODB_DATABASE")
        self.mongodb_collection = os.environ.get("MONGODB_COLLECTION")
        self.mongo_db = MongoDBJobDB(self.mongodb_connection_string, self.mongodb_database) if self.mongodb_connection_string else None
        
        
        # Snowflake connection parameters
        self.snowflake_account = os.environ.get('SNOWFLAKE_ACCOUNT')
        self.snowflake_user = os.environ.get('SNOWFLAKE_USER')
        self.snowflake_password = os.environ.get('SNOWFLAKE_PASSWORD')
        self.snowflake_warehouse = os.environ.get('SNOWFLAKE_WAREHOUSE')
        self.snowflake_database = os.environ.get('SNOWFLAKE_DATABASE')
        self.snowflake_schema = os.environ.get('SNOWFLAKE_SCHEMA')
        self.snowflake_table = os.environ.get('SNOWFLAKE_TABLE')
         
        # Initialize Snowflake connection if all required params are provided
        self.snowflake_conn = None
        if all([self.snowflake_account, self.snowflake_user, self.snowflake_password, self.snowflake_warehouse]):
            try:
                self.snowflake_conn = SnowflakeConnector(
                    self.snowflake_account, 
                    self.snowflake_user, 
                    self.snowflake_password, 
                    self.snowflake_warehouse
                )
                # Set context once
                self.snowflake_conn.execute_query(f"USE DATABASE {self.snowflake_database}")
                self.snowflake_conn.execute_query(f"USE SCHEMA {self.snowflake_schema}")
            except Exception as e:
                print(f"Failed to initialize Snowflake connection: {e}")
                self.snowflake_conn = None
        
        # Weather pipeline queue names
        self.queue_names = {
            'weather_api_requests': 'weather_api_requests_metadata_queue',
            'cloud_upload': 'cloud_upload_queue',
            'deletion': 'deletion_queue',
            'failed_upload': 'failed_upload_queue'
        }

    def create_metrics(self):
        """Create all weather pipeline metrics"""
        
        # Redis Queue Length Metrics
        self.metrics['weather_api_requests_queue_length'] = Gauge(
            'weather_api_requests_queue_length',
            'Number of weather API requests waiting to be processed',
            registry=self.registry
        )
        
        self.metrics['cloud_upload_queue_length'] = Gauge(
            'weather_cloud_upload_queue_length',
            'Number of job IDs waiting to be uploaded to GCS',
            registry=self.registry
        )
        
        self.metrics['deletion_queue_length'] = Gauge(
            'weather_deletion_queue_length',
            'Number of job IDs waiting to be deleted from MongoDB',
            registry=self.registry
        )
        
        self.metrics['failed_upload_queue_length'] = Gauge(
            'weather_failed_upload_queue_length',
            'Number of failed upload job IDs',
            registry=self.registry
        )
        
        # MongoDB Document Count Metrics
        self.metrics['mongodb_weather_documents_count'] = Gauge(
            'mongodb_weather_documents_count',
            'Total number of weather documents in MongoDB',
            registry=self.registry
        )
        
        # Snowflake Row Count Metrics  
        self.metrics['snowflake_weather_rows_count'] = Gauge(
            'snowflake_weather_rows_count',
            'Total number of weather rows in Snowflake',
            registry=self.registry
        )
            
    def get_queue_length(self, queue_name):
        """Get length of a Redis queue"""
        try:
            return self.redis_queue.get_llen(queue_name)
        except Exception as e:
            print(f"Error getting queue length for {queue_name}: {e}")
            return 0
    
    def get_mongodb_document_count(self):
        """Get total count of weather documents in MongoDB using existing connection class"""
        try:
            if self.mongo_db and self.mongodb_collection:
                # Use the num_docs method from your MongoDBConnector base class
                count = self.mongo_db.num_docs(self.mongodb_collection)
                return count
            return 0
        except Exception as e:
            print(f"Error getting MongoDB document count: {e}")
            return 0
    
    def get_snowflake_row_count(self):
        """Get total count of weather rows in Snowflake"""
        try:
            if self.snowflake_conn:
                return self.snowflake_conn.get_table_row_count(
                    self.snowflake_database, 
                    self.snowflake_schema, 
                    self.snowflake_table
                )
            return 0
        except Exception as e:
            print(f"Error getting Snowflake row count: {e}")
            return 0

    def update_snowflake_metrics(self):
        """Update Snowflake metrics separately"""
        snowflake_count = self.get_snowflake_row_count()
        self.metrics['snowflake_weather_rows_count'].set(snowflake_count)

    def update_metrics(self):
        """Update frequent metrics (Redis queues and MongoDB)"""
        
        # Update queue length metrics
        for metric_key, queue_name in self.queue_names.items():
            metric_name = f"{metric_key}_queue_length"
            if metric_name in self.metrics:
                queue_length = self.get_queue_length(queue_name)
                self.metrics[metric_name].set(queue_length)

        # Update MongoDB metrics
        doc_count = self.get_mongodb_document_count()
        self.metrics['mongodb_weather_documents_count'].set(doc_count)


    def run(self):
        """Main execution loop"""
        self.create_metrics()
        print("Weather metrics exporter started...")
        
        round_counter = 18
        
        # Main loop - update every 30 seconds
        while True:
            self.update_metrics()
            
            # Update Snowflake every 20 rounds (20 * 30 seconds = 10 minutes)
            if round_counter % 20 == 0:
                round_counter = 0
                self.update_snowflake_metrics()
            
            round_counter += 1
            time.sleep(30)

    def close_connections(self):
        """Close all connections"""
        if self.redis_queue:
            self.redis_queue.close()
        if self.mongo_db:
            self.mongo_db.close()
        if self.snowflake_conn:
            self.snowflake_conn.close()    

if __name__ == '__main__':
    exporter = WeatherMetricsExporter()
    start_http_server(8000, registry=exporter.registry)
    print("Prometheus metrics server started on port 8000")
    exporter.run()
