# NYC Taxi Modern Lakehouse

A production-grade **Modern Data Lakehouse** built on Snowflake, orchestrated with Apache Airflow, and transformed with dbt. Uses real NYC Yellow Taxi trip data from the NYC Taxi & Limousine Commission (TLC) to demonstrate a complete, end-to-end data engineering platform.

---

## Architecture Overview

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                         NYC TAXI MODERN LAKEHOUSE                               │
├─────────────────────────────────────────────────────────────────────────────────┤
│                                                                                 │
│   NYC TLC API                                                                   │
│  (monthly parquet)                                                              │
│       │                                                                         │
│       ▼                                                                         │
│  ┌──────────────────────────────────────────────────────────────────────────┐   │
│  │                      APACHE AIRFLOW                                      │   │
│  │                                                                          │   │
│  │  ┌──────────────────┐  ┌───────────────────┐  ┌──────────────────────┐  │   │
│  │  │ nyc_taxi_        │  │ nyc_taxi_dbt_     │  │ nyc_taxi_data_       │  │   │
│  │  │ ingestion (@daily│  │ transform (@daily)│  │ quality (@weekly)    │  │   │
│  │  │                  │  │                   │  │                      │  │   │
│  │  │ 1. API health    │  │ 1. Wait sensor    │  │ 1. Bronze freshness  │  │   │
│  │  │ 2. Download      │  │ 2. Test bronze    │  │ 2. Silver nulls      │  │   │
│  │  │ 3. Stage upload  │  │ 3. Run silver     │  │ 3. Gold aggregates   │  │   │
│  │  │ 4. COPY INTO     │  │ 4. Test silver    │  │ 4. Quality report    │  │   │
│  │  │ 5. Validate rows │  │ 5. Run gold       │  │ 5. Alert on failures │  │   │
│  │  │ 6. dbt bronze    │  │ 6. Test gold      │  │                      │  │   │
│  │  │ 7. Notify        │  │ 7. Generate docs  │  │                      │  │   │
│  │  └──────────────────┘  └───────────────────┘  └──────────────────────┘  │   │
│  └──────────────────────────────────────────────────────────────────────────┘   │
│                                   │                                             │
│                                   ▼                                             │
│  ┌──────────────────────────────────────────────────────────────────────────┐   │
│  │                         SNOWFLAKE                                        │   │
│  │                                                                          │   │
│  │  RAW_DATA ──▶  BRONZE  ──▶  SILVER  ──▶  GOLD                           │   │
│  │  (parquet)     (copy+meta)  (clean)      (aggregates)                   │   │
│  │                                                                          │   │
│  │  Transformed by DBT with full testing and documentation                 │   │
│  └──────────────────────────────────────────────────────────────────────────┘   │
│                                   │                                             │
│                                   ▼                                             │
│              BI Tools / Dashboards / ML Feature Stores                          │
│                                                                                 │
└─────────────────────────────────────────────────────────────────────────────────┘
```

---

## Tech Stack

| Layer | Technology | Purpose |
|---|---|---|
| Cloud DW | Snowflake | Storage, compute, and data serving |
| Transformation | dbt 1.7 | SQL transformations, tests, docs |
| Orchestration | Apache Airflow 2.7 | Pipeline scheduling and monitoring |
| Containerization | Docker Compose | Local Airflow environment |
| Source Data | NYC TLC Open Data | Real-world taxi trip records |
| dbt Packages | dbt_utils 1.1.1 | Surrogate keys, date helpers |
| Language | Python 3.10 / SQL | DAGs, macros, transformations |

---

## Data Source

**NYC Yellow Taxi Trip Records** from the NYC Taxi & Limousine Commission (TLC):
- Monthly Parquet files (~500MB–2GB each)
- ~3M records per month, ~160M+ records per year
- Published with ~1-2 month delay
- Download URL: `https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_YYYY-MM.parquet`

**NYC Taxi Zones** — 265 geographic zones used as pickup/dropoff location identifiers:
- CSV available at: `https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv`

---

## Project Structure

```
modern-lakehouse/
│
├── .env.example              # Environment variables template
├── docker-compose.yml        # Airflow stack (webserver, scheduler, postgres)
├── requirements.txt          # Python dependencies
├── README.md                 # This file
│
├── dbt_project/
│   ├── dbt_project.yml       # dbt project configuration
│   ├── profiles.yml          # Snowflake connection profile (uses env vars)
│   ├── packages.yml          # dbt package dependencies
│   │
│   ├── models/
│   │   ├── bronze/
│   │   │   ├── bronze_taxi_trips.sql   # Raw trips + metadata
│   │   │   ├── bronze_taxi_zones.sql   # Raw zones + metadata
│   │   │   └── schema.yml              # Sources, tests, docs
│   │   │
│   │   ├── silver/
│   │   │   ├── silver_taxi_trips.sql   # Cleaned, typed, enriched trips
│   │   │   ├── silver_taxi_zones.sql   # Standardized zone reference
│   │   │   └── schema.yml              # Tests, docs, relationships
│   │   │
│   │   └── gold/
│   │       ├── gold_daily_trip_summary.sql   # Daily KPIs by borough
│   │       ├── gold_hourly_demand.sql        # Hourly demand patterns
│   │       ├── gold_zone_performance.sql     # Per-zone metrics
│   │       ├── gold_driver_economics.sql     # Vendor economics
│   │       └── schema.yml                   # Tests and docs
│   │
│   ├── macros/
│   │   ├── clean_string.sql             # TRIM(UPPER(col)) helper
│   │   ├── is_valid_coordinates.sql     # NYC bounds validator
│   │   └── generate_schema_name.sql     # Override dbt schema naming
│   │
│   └── tests/
│       ├── assert_silver_trips_no_future_dates.sql
│       ├── assert_gold_revenue_positive.sql
│       └── assert_trip_duration_reasonable.sql
│
├── airflow/
│   ├── dags/
│   │   ├── dag_ingest_nyc_taxi.py      # Daily ingestion pipeline
│   │   ├── dag_transform_dbt.py        # dbt transformation pipeline
│   │   └── dag_data_quality.py         # Weekly quality monitoring
│   └── plugins/                        # Custom Airflow plugins (extendable)
│
├── data/                               # Local data files (gitignored)
│
└── docs/
    ├── snowflake_setup.sql             # Complete Snowflake setup script
    └── architecture.md                 # Detailed architecture documentation
```

---

## Setup Instructions

### Prerequisites

- Docker Desktop (for Airflow)
- Python 3.10+
- A Snowflake account (free trial at [snowflake.com](https://signup.snowflake.com/))
- `snowsql` CLI (optional, for running setup script)

---

### Step 1: Clone and Configure Environment

```bash
# Navigate to project directory
cd /Users/satishvarma/Desktop/modern-lakehouse

# Copy environment template
cp .env.example .env

# Edit .env with your actual Snowflake credentials
# Required fields:
#   SNOWFLAKE_ACCOUNT    (e.g., xy12345.us-east-1)
#   SNOWFLAKE_USER       nyc_taxi_user
#   SNOWFLAKE_PASSWORD   (set in Step 2)
#   SNOWFLAKE_ROLE       nyc_taxi_role
#   SNOWFLAKE_WAREHOUSE  nyc_taxi_wh
#   SNOWFLAKE_DATABASE   nyc_taxi_lakehouse
#   SNOWFLAKE_SCHEMA     raw_data
nano .env
```

---

### Step 2: Snowflake Setup

Run the complete setup script in your Snowflake worksheet:

```sql
-- Option A: Snowflake Web UI
-- Copy the contents of docs/snowflake_setup.sql into a Snowflake worksheet
-- Execute all sections top to bottom

-- Option B: SnowSQL CLI
snowsql -a <your_account> -u <admin_user> -f docs/snowflake_setup.sql
```

**Important:** Change the password in `snowflake_setup.sql` line:
```sql
PASSWORD = 'ChangeMe_Secure_Password_123!'
```
Set it to a secure value and update your `.env` accordingly.

---

### Step 3: dbt Setup

```bash
# Install Python dependencies
pip install -r requirements.txt

# Install dbt packages
cd dbt_project
dbt deps

# Test Snowflake connection
dbt debug --profiles-dir . --project-dir .

# Expected output:
# Connection test: [OK connection ok]
```

---

### Step 4: Airflow Setup

```bash
cd /Users/satishvarma/Desktop/modern-lakehouse

# Initialize the Airflow database and create admin user
docker-compose up airflow-init

# Start all Airflow services
docker-compose up -d

# Verify all containers are running
docker-compose ps
```

Access the Airflow UI at `http://localhost:8080`
- Username: `airflow`
- Password: `airflow`

**Configure Snowflake Connection in Airflow:**

1. Go to Admin > Connections
2. Click "+" to add a new connection
3. Fill in:
   - Connection ID: `snowflake_default`
   - Connection Type: `Snowflake`
   - Account: your Snowflake account identifier
   - Login: `nyc_taxi_user`
   - Password: your password
   - Schema: `raw_data`
   - Warehouse: `nyc_taxi_wh`
   - Database: `nyc_taxi_lakehouse`
   - Role: `nyc_taxi_role`

---

### Step 5: Load NYC Taxi Zone Data (One-time seed)

Download and load the zone lookup table before running DAGs:

```bash
# Download the zone CSV
curl -o /tmp/taxi_zone_lookup.csv \
  "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv"

# Load to Snowflake (SnowSQL)
snowsql -a <account> -u nyc_taxi_user -q "
    PUT 'file:///tmp/taxi_zone_lookup.csv' @~/taxi_stage/ AUTO_COMPRESS=TRUE;
    COPY INTO nyc_taxi_lakehouse.raw_data.nyc_taxi_zones
    FROM @~/taxi_stage/taxi_zone_lookup.csv.gz
    FILE_FORMAT = (TYPE='CSV' FIELD_OPTIONALLY_ENCLOSED_BY='\"' SKIP_HEADER=1)
    MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE;
"
```

---

## How to Run

### Run the Full Pipeline Manually

```bash
# Option A: Via Airflow UI
# 1. Go to http://localhost:8080
# 2. Enable and trigger 'nyc_taxi_ingestion' DAG
# 3. Wait for completion, then trigger 'nyc_taxi_dbt_transform'

# Option B: Run dbt directly (after ingestion)
cd dbt_project
dbt run --profiles-dir . --project-dir .     # Run all models
dbt test --profiles-dir . --project-dir .    # Run all tests
dbt docs generate --profiles-dir . --project-dir .
dbt docs serve   # Opens browser at http://localhost:8080
```

### Run Individual Layers

```bash
cd dbt_project

# Bronze only
dbt run --select bronze --profiles-dir . --project-dir .

# Silver only
dbt run --select silver --profiles-dir . --project-dir .

# Gold only
dbt run --select gold --profiles-dir . --project-dir .

# Specific model
dbt run --select gold_daily_trip_summary --profiles-dir . --project-dir .

# Run tests for a single model
dbt test --select silver_taxi_trips --profiles-dir . --project-dir .
```

### Run Data Quality Checks

```bash
# Via Airflow UI: trigger 'nyc_taxi_data_quality' DAG manually
# Or run dbt tests directly:
cd dbt_project
dbt test --profiles-dir . --project-dir .
```

---

## Data Lineage

```
NYC TLC Parquet Files
        │
        │  Airflow: COPY INTO
        ▼
raw_data.nyc_taxi_trips ──────────────────────────────────────────────────┐
raw_data.nyc_taxi_zones ──────────────────────────────────────────────────┤
                                                                          │
                                ┌─────────────────────────────────────────┘
                                │  dbt ref()
                                ▼
        bronze_taxi_trips ◀── raw_data.nyc_taxi_trips
        bronze_taxi_zones ◀── raw_data.nyc_taxi_zones
                │                       │
                │  dbt ref()            │  dbt ref()
                ▼                       ▼
        silver_taxi_trips ◀──── silver_taxi_zones
                │                       │
                └──────────┬────────────┘
                           │  dbt ref()
                    ┌──────┴──────┬──────────────┬─────────────────┐
                    ▼             ▼               ▼                 ▼
         gold_daily_trip_  gold_hourly_  gold_zone_       gold_driver_
         summary           demand        performance      economics
```

**Layer descriptions:**

| Layer | Model | Description |
|---|---|---|
| Raw | `nyc_taxi_trips` | Parquet columns loaded verbatim via COPY INTO |
| Raw | `nyc_taxi_zones` | Zone CSV loaded verbatim |
| Bronze | `bronze_taxi_trips` | Raw + `_loaded_at`, `_source`, `_file_name` |
| Bronze | `bronze_taxi_zones` | Raw + metadata columns |
| Silver | `silver_taxi_trips` | Typed, filtered, surrogate key, decoded columns |
| Silver | `silver_taxi_zones` | Trimmed strings, `borough_code`, `is_airport` |
| Gold | `gold_daily_trip_summary` | Daily KPIs by date × borough |
| Gold | `gold_hourly_demand` | Hourly demand with peak_hour_flag |
| Gold | `gold_zone_performance` | Per-zone pickups, dropoffs, rankings, destinations |
| Gold | `gold_driver_economics` | Vendor × payment type fare and tip efficiency |

---

## Key dbt Models

### `silver_taxi_trips`
- **Grain:** One row per taxi trip
- **Key transforms:** Surrogate key, type casting, business rule filtering, decoded categoricals
- **Tests:** `unique`, `not_null`, `accepted_values`, `relationships`, `expression_is_true`

### `gold_daily_trip_summary`
- **Grain:** One row per (trip_date, pickup_borough)
- **Key metrics:** total_trips, total_revenue, avg_fare, avg_duration, airport_trip_pct

### `gold_hourly_demand`
- **Grain:** One row per (trip_hour, day_of_week, pickup_borough)
- **Key features:** peak_hour_flag (top 20% by volume), avg_wait_efficiency

### `gold_zone_performance`
- **Grain:** One row per zone_id (265 rows)
- **Key features:** revenue_rank within borough, most_common_destination, trip_count_last_30_days

---

## Development Workflow

```bash
# 1. Create a feature branch
git checkout -b feature/add-green-taxi

# 2. Make model changes in dbt_project/models/

# 3. Run and test locally
cd dbt_project
dbt run --select <model_name> --profiles-dir . --project-dir .
dbt test --select <model_name> --profiles-dir . --project-dir .

# 4. Check lineage
dbt docs generate && dbt docs serve

# 5. Open PR for review
```

---

## Troubleshooting

**`dbt debug` fails with connection error:**
- Verify all environment variables in `.env` are set correctly
- Check your Snowflake account identifier format: `xy12345.us-east-1` (no `.snowflakecomputing.com`)
- Ensure `nyc_taxi_wh` warehouse is not suspended: `ALTER WAREHOUSE nyc_taxi_wh RESUME;`

**Airflow DAG not appearing in UI:**
- Check DAG syntax: `python airflow/dags/dag_ingest_nyc_taxi.py`
- Check Airflow logs: `docker-compose logs airflow-scheduler`

**COPY INTO returns zero rows:**
- Verify the parquet file is in the stage: `LIST @~/taxi_stage/;`
- Check file format matches: `SHOW FILE FORMATS IN SCHEMA nyc_taxi_lakehouse.raw_data;`

**dbt test failures on `relationships`:**
- The zone CSV must be loaded before running silver models
- Run: `SELECT COUNT(*) FROM nyc_taxi_lakehouse.silver.silver_taxi_zones;` — should be 265

---

## Contributing

1. Follow the existing model naming convention: `<layer>_<subject>_<optional_qualifier>.sql`
2. Every new model needs a corresponding entry in its layer's `schema.yml`
3. All primary keys must have `unique` and `not_null` tests
4. Document columns with descriptions in `schema.yml`
5. Run `dbt test` before opening a PR — no failing tests allowed

---

## License

This project is provided for educational purposes. NYC TLC data is public domain.
