# WeatherFlow - Weather Data Pipeline

A complete weather data pipeline that fetches data from OpenWeather API, stores it in the cloud, and runs analytics. Built with Airflow, MongoDB, Redis, GCS, and Snowflake.

## How It Works

The pipeline runs in 4 stages:

1. **Collect**: Fetch weather data from OpenWeather API for specified locations
2. **Transfer**: Move data from MongoDB to Google Cloud Storage as Parquet files
3. **Load**: Import data from GCS into Snowflake data warehouse
4. **Analyze**: Run daily analytics and generate insights

All stages are orchestrated by Apache Airflow and use Redis queues for job coordination.

## Project Structure

```
weatherflow/
├── airflow/                    # Airflow configuration and DAGs
│   ├── dags/                   # The main pipeline workflows
│   ├── logs/                   # Airflow execution logs
│   ├── config/                 # Airflow settings
│   └── plugins/                # Custom Airflow plugins
├── data/                       # Location data (cities and coordinates)
├── infra/                      # Database and cloud connection modules
├── monitor/                    # Grafana dashboards and monitoring
└── docker-compose.yaml         # Infrastructure setup
```

### Key Directories

**`airflow/dags/`** - The heart of the pipeline:
- `collect_openweather_api_to_mongodb.py` - Fetches weather data from API
- `transfer_mongodb_to_gcs.py` - Moves data to cloud storage
- `load_gcs_to_snowflake.py` - Loads data into Snowflake
- `analyze_weather_daily_snowflake.py` - Runs daily analytics

**`infra/`** - Connection modules for different systems:
- `connections_mongodb.py` - MongoDB operations
- `connections_redis.py` - Redis queue management
- `connections_gcs.py` - Google Cloud Storage uploads
- `connections_snowflake.py` - Snowflake data warehouse operations

**`data/`** - Contains CSV files with city coordinates for weather collection

**`monitor/`** - Grafana dashboards for monitoring pipeline health and metrics

## Pipeline Details

### 1. Data Collection (collect_openweather_api_to_mongodb.py)

**Schedule**: Every hour (`0 * * * *`)

**What it does**:
- Reads city coordinates from CSV file
- Creates job queue in Redis for each location
- Parallel workers fetch weather data from OpenWeather API
- Stores complete API responses in MongoDB
- Queues successful jobs for next stage

**Key features**:
- Parallel processing with 2 workers
- Retry logic for API failures
- Comprehensive data extraction (temperature, humidity, wind, etc.)
- Job tracking with unique IDs

### 2. Data Transfer (transfer_mongodb_to_gcs.py)

**Schedule**: Every 3 hours (`15 */3 * * *`)

**What it does**:
- Processes job IDs from Redis upload queue
- Fetches weather records from MongoDB
- Converts data to Parquet format
- Uploads to Google Cloud Storage with date partitioning
- Cleans up MongoDB after successful upload

**Key features**:
- Batch processing for efficiency
- Date-partitioned storage (`year=2025/month=06/day=15/`)
- Data transformation and cleaning
- Failed job tracking and retry

### 3. Data Loading (load_gcs_to_snowflake.py)

**Schedule**: Every 6 hours (`30 */6 * * *`)

**What it does**:
- Scans GCS bucket for new Parquet files
- Loads data into Snowflake using COPY INTO
- Tracks processed files to avoid duplicates
- Manages Snowflake table schema

**Key features**:
- Automatic schema detection
- Incremental loading
- File processing tracking
- Error handling for schema mismatches

### 4. Analytics (analyze_weather_daily_snowflake.py)

**Schedule**: Daily at 11 PM (`0 23 * * *`)

**What it does**:
- Runs daily weather summary queries
- Identifies temperature extremes
- Analyzes weather condition patterns
- Logs insights and data quality metrics (check log part of related DAG on airflow web UI)

**Sample output**:
```
🌡️ Daily Weather Insights - 2025-06-15
========================================
🔥 Hottest: Madrid (28.5°C)
❄️ Coldest: Stockholm (12.3°C)
☁️ Weather Conditions:
   Clear: 15 readings
   Clouds: 8 readings
   Rain: 2 readings
📋 Data Summary:
   Total readings: 25
   Locations: 7
   Global average: 20.1°C
```

## Technology Stack

**Orchestration**: Apache Airflow with LocalExecutor
**Queuing**: Redis for job coordination
**Staging**: MongoDB for temporary storage
**Cloud Storage**: Google Cloud Storage (Parquet format)
**Data Warehouse**: Snowflake
**Monitoring**: Grafana + Prometheus
**Infrastructure**: Docker Compose

## Data Flow

1. **Schedule Jobs** → Redis queue with city coordinates
2. **API Collection** → MongoDB (raw JSON responses)
3. **Cloud Transfer** → GCS (Parquet files, date-partitioned)
4. **Data Loading** → Snowflake (structured tables)
5. **Analytics** → Daily insights and summaries

## How to Run

1. **Clone the repository**:
   ```bash
   git clone <repo-url>
   cd weatherflow
   ```

2. **Setup environment**:
   - Create a `.env` file in the main directory with all required credentials:
   ```bash
   # Airflow Configuration
   AIRFLOW_UID=50000
   _AIRFLOW_WWW_USER_USERNAME=airflow
   _AIRFLOW_WWW_USER_PASSWORD=airflow123

   # OpenWeather API Configuration
   OPENWEATHER_API_KEY=your_openweather_api_key_here

   # MongoDB Configuration
   MONGODB_CONNECTION_STRING=mongodb://admin:password123@weatherflow-mongo:27017/weather_db?authSource=admin
   MONGODB_DATABASE=weather_db
   MONGODB_COLLECTION=daily_weather

   # Redis Configuration
   REDIS_HOST=weatherflow-redis 
   REDIS_PORT=6379              
   REDIS_DB=0

   # GCP Configuration
   GCP_PROJECT_ID=your_gcp_project_id
   GCS_BUCKET_NAME=your_gcs_bucket_name
   GCS_FOLDER_NAME=weather-data
   GCP_CREDENTIALS_PATH=/opt/airflow/project/gcp-credentials.json

   # Snowflake Configuration
   SNOWFLAKE_ACCOUNT=your_snowflake_account
   SNOWFLAKE_USER=your_snowflake_username
   SNOWFLAKE_PASSWORD=your_snowflake_password
   SNOWFLAKE_WAREHOUSE=COMPUTE_WH
   SNOWFLAKE_DATABASE=WEATHER_DB
   SNOWFLAKE_SCHEMA=RAW_DATA
   SNOWFLAKE_TABLE=WEATHER_DATA
   ```
   
   - Add your `gcp-credentials.json` file to the main directory (download from your GCP service account)

3. **Start infrastructure**:
   ```bash
   ./setup-docker.sh
   docker-compose up -d
   ```

4. **Access Airflow**:
   - URL: http://localhost:8080
   - Username: airflow
   - Password: airflow123

5. **Monitor pipeline**:
   - Grafana: http://localhost:3000
   - Redis: localhost:6381
   - MongoDB: localhost:27019


## Monitoring

The system includes monitoring with two main parts:

### Grafana Dashboards
- **URL**: http://localhost:3000
- **Infrastructure Dashboard**: System health and resource usage
- **Main Dashboard**: Pipeline monitoring and metrics

### Monitoring Infrastructure

**Custom Metrics Exporter** (`weatherflow-custom-exporter`):
- Tracks pipeline metrics:
  - Redis queue lengths (API requests, uploads, deletions, failed jobs)
  - MongoDB document counts
  - Snowflake row counts
- Updates every 30 seconds (Snowflake every 10 minutes)

**System Monitoring**:
- **Node Exporter**: Hardware metrics (CPU, memory, disk, network)
- **cAdvisor**: Docker container metrics (resource usage, performance)
- **Prometheus**: Data collection and storage with 1-year retention


## Development and Design Notes

### Pipeline Scalability
* This pipeline can handle data from all cities and towns in the world, but is limited to less than 40 cities due to free tier API limitations (1000 requests per day)
* Redis queuing implements worker/consumer logic for load balancing. Can be replaced with Kafka/Celery if needed, but increasing worker count is enough for current usage

### MongoDB Storage Decision
* MongoDB serves as intermediary storage to separate data extraction from loading
* If GCS has problems, data extraction keeps running without stopping
* When the data source changes format, MongoDB saves it without schema issues until we fix other pipeline parts
* Prevents data loss and handles schema changes easily

### Parquet File Choice
* Used Parquet format because of compression efficiency
* Snowflake can directly insert data from Parquet files
* Good for analytics workloads

### Snowflake Loading Method
* File copying is faster than streaming, so used that approach
* Built two different tasks for inserting data:
  1. **Direct insertion**: Only prevents duplicates based on files (no "on conflict" like PostgreSQL)
  2. **Staging + merge**: Uses staging table then merges for proper deduplication
* Researched best ways to handle duplicates in Snowflake and created the staging/merge solution
* Both methods handle new fields without breaking when schema changes


---
================================================================================
---

# TAKE-HOME PROJECT QUESTIONS

Below are the answers to the specific questions for the project
# Architecture Design

## 1. Loading Data from Cloud Storage into Snowflake

### Current Implementation: File-Based Loading

I implemented two approaches for loading data from GCS to Snowflake:

#### Option A: Direct COPY INTO (Current Primary Method)
**How it works:**
- Use Snowflake's `COPY INTO` command to load Parquet files directly from GCS
- Track processed files to avoid duplicates
- Auto-detect schema changes

**Pros:**
- Fast loading for large files
- Cost-effective (only pay for compute during load)
- Native Snowflake feature with good performance
- Handles schema evolution automatically
- Simple error handling

**Cons:**
- No built-in deduplication at row level
- File-based duplicate prevention only
- Requires manual tracking of processed files

#### Option B: Staging Table + MERGE (Implemented as Alternative)
**How it works:**
- Load data into staging table first
- Use MERGE statement to upsert into final table
- Proper row-level deduplication

**Pros:**
- Handles duplicates properly at row level
- Better data quality control
- Can implement business logic during merge
- Supports updates and deletes

**Cons:**
- Higher cost (double storage temporarily)
- More complex logic
- Slower for large datasets
- Higher compute usage

### Other Options Considered

#### Option C: Snowpipe (Auto-ingestion)
**How it works:**
- Automatic ingestion when files land in GCS
- Event-driven loading

**Pros:**
- Near real-time loading
- No manual orchestration needed
- Low latency

**Cons:**
- Higher cost for small files
- Less control over loading process
- Harder to implement complex transformations
- Not suitable for batch processing patterns

#### Option D: Streaming (Kafka/Kinesis)
**How it works:**
- Stream data directly to Snowflake
- Real-time ingestion

**Pros:**
- Lowest latency
- Real-time analytics possible

**Cons:**
- Much higher cost
- Complex infrastructure
- Overkill for weather data (not high-frequency)
- Requires additional streaming infrastructure

### Recommendation for This Use Case
I chose **Option A (Direct COPY INTO)** as primary because:
- Weather data is batch-oriented (hourly collection)
- Cost-effective for this data volume
- Simple to maintain and debug
- Good performance for analytics workloads

Option B is available for cases where row-level deduplication is critical.

## 2. Transforming Raw Data into Insights

#### High-Level Plan for Insights

**Phase 1: Basic Analytics (Current)**
```
Raw Weather Data → Daily Aggregations → Simple Insights
```
- Daily temperature summaries by location
- Weather condition frequency
- Data quality metrics

**Phase 2: Enhanced Analytics (Next Steps)**
```
Raw Data → Dimensional Model → Business Metrics → Dashboards
```

**Dimensional Model Design:**
- **Fact Table**: `fact_weather_readings`
  - Measurements (temperature, humidity, pressure)
  - Foreign keys to dimensions
  - Timestamp information

- **Dimension Tables**:
  - `dim_location`: City details, coordinates, country
  - `dim_date`: Date hierarchies (day, week, month, season)
  - `dim_weather_condition`: Weather types and categories

**Phase 3: Advanced Analytics**
- **Historical Trends**: Temperature patterns over time
- **Seasonal Analysis**: Weather patterns by season/month
- **Anomaly Detection**: Unusual weather events
- **Comparative Analysis**: City-to-city weather comparisons
- **Forecasting**: Simple prediction models using historical data

### Tools for Each Phase

#### Data Transformation
- **Current**: Native Snowflake SQL
- **Future**: dbt for complex transformations
- **Reason**: dbt provides version control, testing, documentation for SQL

#### Analytics and BI
- **Current**: Grafana for basic dashboards
- **Future Options**:
  - Tableau/Power BI for business users
  - Jupyter notebooks for data science
  - Custom dashboards for specific use cases

#### Data Quality and Monitoring
- **Current**: Custom metrics via Prometheus
- **Future**: 
  - dbt tests for data quality
  - Great Expectations for data validation
  - Automated alerting for data issues

### Orchestration Strategy

#### Current Approach
- Airflow DAGs for each stage
- Queue-based coordination with Redis
- Simple scheduling (hourly, daily)

#### Enhanced Orchestration
- **dbt integration**: Run transformations as Airflow tasks
- **Data lineage**: Track data dependencies
- **Dynamic scheduling**: Based on data availability
- **Backfill capabilities**: Handle historical data processing

### Scalability Considerations

#### For Higher Data Volumes
- **Partitioning**: Snowflake table clustering
- **Parallel Processing**: Multiple Airflow workers
- **Incremental Loading**: Only process new/changed data
- **Caching**: Materialized views for frequent queries

#### For More Complex Analytics
- **Spark Integration**: For heavy transformations
- **ML Pipeline**: Snowflake ML functions or external tools
- **Real-time Layer**: Add streaming for urgent insights
