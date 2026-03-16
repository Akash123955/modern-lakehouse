# NYC Taxi Modern Lakehouse - Architecture Guide

## Table of Contents
1. [Overview](#overview)
2. [Medallion Architecture](#medallion-architecture)
3. [Data Flow Diagram](#data-flow-diagram)
4. [Technology Stack](#technology-stack)
5. [Snowflake Schema Design](#snowflake-schema-design)
6. [dbt Model Dependency Graph](#dbt-model-dependency-graph)
7. [Airflow DAG Descriptions](#airflow-dag-descriptions)
8. [Data Quality Strategy](#data-quality-strategy)
9. [Extending the Project](#extending-the-project)

---

## Overview

This project implements a **production-grade Modern Data Lakehouse** on Snowflake using
the Medallion Architecture pattern. Raw NYC Yellow Taxi trip data is ingested daily from
the NYC Taxi & Limousine Commission (TLC) public dataset, cleaned and enriched through
multiple transformation layers, and surfaced as analytics-ready aggregates for BI and ML use cases.

The pipeline is orchestrated by Apache Airflow and all transformations are implemented
as dbt models, enabling version-controlled, tested, and documented data transformations.

---

## Medallion Architecture

The Medallion Architecture organizes data into three progressive quality layers:

```
┌─────────────────────────────────────────────────────────────────────┐
│                      MEDALLION ARCHITECTURE                         │
├─────────────────────────────────────────────────────────────────────┤
│                                                                     │
│  ┌──────────────┐    ┌──────────────┐    ┌──────────────────────┐  │
│  │   BRONZE     │    │    SILVER    │    │        GOLD          │  │
│  │  (Raw Copy)  │───▶│  (Cleaned)  │───▶│    (Aggregated)      │  │
│  └──────────────┘    └──────────────┘    └──────────────────────┘  │
│                                                                     │
│  • Minimal change    • Typed columns    • Business aggregates       │
│  • Metadata added    • Rules applied    • Analytics-ready           │
│  • Full history      • No invalid data  • Joined/enriched           │
│  • Append-only       • Deduplicated     • KPI metrics               │
│                                                                     │
└─────────────────────────────────────────────────────────────────────┘
```

### Bronze Layer (Raw Ingestion)

**Purpose:** Preserve the raw source data with minimal transformation.

**What happens here:**
- Data is loaded via Airflow's COPY INTO command from Snowflake internal stages
- Bronze dbt models select all columns from `RAW_DATA.*` tables
- Three metadata columns are appended: `_loaded_at`, `_source`, `_file_name`
- No filtering, no type casting, no business logic

**Why it matters:** Bronze serves as the single source of truth. If downstream logic is wrong,
we can always reprocess from bronze without re-ingesting from the source API.

**Models:**
- `bronze_taxi_trips` — all NYC Yellow Taxi trip records
- `bronze_taxi_zones` — TLC zone lookup table

### Silver Layer (Cleaned & Typed)

**Purpose:** Apply data quality rules and business transformations to produce a reliable,
typed dataset suitable for analytical queries.

**What happens here:**
- All columns are cast to precise types (`TIMESTAMP_NTZ`, `NUMERIC(10,2)`, `INTEGER`)
- Invalid trips are filtered: zero distance, zero fare, invalid passenger counts, reversed timestamps
- Derived columns are calculated: `trip_duration_minutes`, `trip_speed_mph`, `tip_pct`
- Categorical codes are decoded: `payment_type_desc`, `vendor_name`
- Boolean flags are added: `is_airport_trip`
- A surrogate `trip_id` is generated using `dbt_utils.generate_surrogate_key`
- String columns are standardized: trimmed and uppercased via the `clean_string` macro

**Why it matters:** All business logic lives here. Data consumers can trust that silver data
is clean, typed, and ready to query without defensive casting.

**Models:**
- `silver_taxi_trips` — cleaned, enriched trip records
- `silver_taxi_zones` — standardized zone reference data

### Gold Layer (Analytics-Ready)

**Purpose:** Pre-aggregate and denormalize data into business-domain-specific models
optimized for BI tools, dashboards, and ML feature stores.

**What happens here:**
- Joins are materialized (trips + zones = borough context)
- Aggregations are computed (daily KPIs, hourly demand patterns)
- Window functions calculate rankings, percentiles, and shares
- Business metrics are named clearly for non-technical users

**Models:**
- `gold_daily_trip_summary` — daily KPIs by date and borough
- `gold_hourly_demand` — demand patterns by hour, day, and borough
- `gold_zone_performance` — zone-level pickup/dropoff metrics
- `gold_driver_economics` — vendor and payment type economics

---

## Data Flow Diagram

```
 NYC TLC Public Data
 (CloudFront CDN)
       │
       │  HTTP GET (monthly parquet)
       ▼
┌─────────────────┐
│  Airflow DAG:   │
│  nyc_taxi_      │
│  ingestion      │
│                 │
│  1. Health ✓    │
│  2. Download    │
│  3. PUT stage   │
│  4. COPY INTO   │
│  5. Validate    │
│  6. dbt bronze  │
└────────┬────────┘
         │
         │  COPY INTO (Parquet → Snowflake)
         ▼
┌─────────────────────────────────────────────────────────────────┐
│                    SNOWFLAKE                                    │
│                                                                 │
│  RAW_DATA Schema                                                │
│  ┌──────────────────────┐  ┌──────────────────────┐            │
│  │  nyc_taxi_trips      │  │  nyc_taxi_zones       │           │
│  │  (raw parquet cols)  │  │  (locationid, borough │           │
│  │                      │  │   zone, service_zone) │           │
│  └──────────┬───────────┘  └──────────┬────────────┘           │
│             │                         │                         │
│             │  dbt run --select bronze│                         │
│             ▼                         ▼                         │
│  BRONZE Schema                                                  │
│  ┌──────────────────────┐  ┌──────────────────────┐            │
│  │ bronze_taxi_trips    │  │ bronze_taxi_zones     │           │
│  │ + _loaded_at         │  │ + _loaded_at          │           │
│  │ + _source            │  │ + _source             │           │
│  │ + _file_name         │  │ + _file_name          │           │
│  └──────────┬───────────┘  └──────────┬────────────┘           │
│             │                         │                         │
│             │  dbt run --select silver│                         │
│             ▼                         ▼                         │
│  SILVER Schema                                                  │
│  ┌──────────────────────────────────────────────┐              │
│  │ silver_taxi_trips                            │              │
│  │ • Typed columns (TIMESTAMP_NTZ, NUMERIC)     │              │
│  │ • trip_id (surrogate key)                    │              │
│  │ • trip_duration_minutes, trip_speed_mph      │              │
│  │ • payment_type_desc, vendor_name decoded     │              │
│  │ • is_airport_trip flag                       │              │
│  │ • Invalid trips filtered out                 │              │
│  └──────────────────────┬───────────────────────┘              │
│                         │                                       │
│  ┌──────────────────────┘                                       │
│  │    ┌──────────────────────────────────────────┐              │
│  │    │ silver_taxi_zones                        │              │
│  │    │ • Trimmed/uppercased strings             │              │
│  │    │ • borough_code (numeric)                 │              │
│  │    │ • is_airport flag                        │              │
│  │    └──────────────────────┬───────────────────┘              │
│  │                           │                                  │
│  └──────────┬────────────────┘                                  │
│             │  dbt run --select gold                            │
│             ▼                                                   │
│  GOLD Schema                                                    │
│  ┌──────────────────────┐  ┌──────────────────────┐            │
│  │ gold_daily_trip_     │  │ gold_hourly_demand   │            │
│  │ summary              │  │ • hour, day_of_week  │            │
│  │ • date, borough      │  │ • borough            │            │
│  │ • KPI aggregates     │  │ • peak_hour_flag     │            │
│  └──────────────────────┘  └──────────────────────┘            │
│  ┌──────────────────────┐  ┌──────────────────────┐            │
│  │ gold_zone_performance│  │ gold_driver_economics│            │
│  │ • per-zone metrics   │  │ • vendor, payment    │            │
│  │ • rankings           │  │ • fare efficiency    │            │
│  └──────────────────────┘  └──────────────────────┘            │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
         │
         │  BI / Dashboards / ML Features
         ▼
    Analytics Consumers
    (Tableau, Looker, Jupyter, etc.)
```

---

## Technology Stack

| Component | Technology | Version | Rationale |
|---|---|---|---|
| **Cloud Data Warehouse** | Snowflake | Current | Separation of compute/storage, zero-copy cloning, native Parquet COPY INTO, time travel |
| **Transformation Framework** | dbt (data build tool) | 1.7.x | SQL-native transformations, testing, documentation, lineage tracking |
| **Workflow Orchestration** | Apache Airflow | 2.7.x | Industry standard, rich operator ecosystem, Snowflake provider |
| **Containerization** | Docker Compose | - | Reproducible local Airflow environment |
| **Source Data** | NYC TLC Open Data | Monthly releases | Free, real-world public dataset with rich schema |
| **dbt Packages** | dbt_utils | 1.1.1 | Surrogate keys, date functions, generic test helpers |
| **Language** | Python 3.10+ / SQL | - | Industry standard for data engineering |

### Why Snowflake?

- **Elasticity:** X-SMALL warehouse for dev/test; scale up for heavy loads without downtime
- **Parquet-native:** COPY INTO directly reads Parquet without an intermediate CSV step
- **RBAC:** Fine-grained role-based access control for multi-team environments
- **Time Travel:** 90-day history enables point-in-time queries and recovery
- **Zero-Copy Cloning:** Clone entire schemas instantly for dev/test environments
- **dbt compatibility:** First-class support with `dbt-snowflake` adapter

### Why dbt?

- **Version-controlled SQL:** All transformations in Git, reviewed via PR
- **Built-in testing:** `not_null`, `unique`, `accepted_values`, `relationships` tests
- **Auto-generated documentation:** `dbt docs generate` produces a data catalog
- **Lineage graph:** Visual DAG of model dependencies via `dbt docs serve`
- **Modularity:** `ref()` macro enables reusable, composable models
- **Macros:** Jinja templating for reusable SQL logic (e.g., `clean_string`, `generate_schema_name`)

### Why Airflow?

- **Dependency management:** ExternalTaskSensor ensures transform DAG only runs after ingestion
- **Retries:** Configurable retry logic with exponential backoff
- **Snowflake operator:** Native Snowflake integration via `apache-airflow-providers-snowflake`
- **Monitoring:** Built-in UI for DAG runs, task logs, and alerting
- **Scheduling:** Flexible cron/preset schedules per DAG

---

## Snowflake Schema Design

```
DATABASE: nyc_taxi_lakehouse
│
├── SCHEMA: raw_data
│   ├── TABLE: nyc_taxi_trips     (raw parquet columns, no constraints)
│   ├── TABLE: nyc_taxi_zones     (locationid, borough, zone, service_zone)
│   ├── STAGE: taxi_stage         (internal stage for Airflow PUT)
│   └── FILE FORMAT: parquet_format
│
├── SCHEMA: bronze
│   ├── TABLE: bronze_taxi_trips  (raw_data.nyc_taxi_trips + metadata)
│   └── TABLE: bronze_taxi_zones  (raw_data.nyc_taxi_zones + metadata)
│
├── SCHEMA: silver
│   ├── TABLE: silver_taxi_trips  (typed, filtered, enriched)
│   └── TABLE: silver_taxi_zones  (cleaned strings, borough_code, is_airport)
│
└── SCHEMA: gold
    ├── TABLE: gold_daily_trip_summary   (date x borough aggregates)
    ├── TABLE: gold_hourly_demand        (hour x day x borough patterns)
    ├── TABLE: gold_zone_performance     (per-zone metrics and rankings)
    └── TABLE: gold_driver_economics     (vendor x payment type economics)
```

**Key Design Decisions:**

1. **Separate schemas per layer** — each schema has distinct RBAC grants. Read-only BI users
   only get access to GOLD. ETL service accounts have write access to all layers.

2. **No foreign keys enforced** — Snowflake does not enforce FK constraints. Data integrity
   is ensured by dbt's `relationships` test instead, which catches violations in CI.

3. **TIMESTAMP_NTZ** — All timestamps use TIMESTAMP_NTZ (No Time Zone) to avoid DST
   ambiguity. NYC taxi data is implicitly Eastern Time; this is documented in column descriptions.

4. **NUMERIC(10,2) for money** — Prevents floating-point precision errors in financial columns.

5. **Surrogate keys in silver** — `dbt_utils.generate_surrogate_key` creates stable trip IDs
   from composite business keys since the raw data has no natural primary key.

---

## dbt Model Dependency Graph

```
  [Sources]
  raw_data.nyc_taxi_trips   raw_data.nyc_taxi_zones
          │                          │
          ▼                          ▼
  bronze_taxi_trips         bronze_taxi_zones
          │                          │
          ▼                          ▼
  silver_taxi_trips ◀────── silver_taxi_zones
          │                          │
          └────────────┬─────────────┘
                       │
          ┌────────────┼────────────┬────────────────┐
          ▼            ▼            ▼                 ▼
  gold_daily_    gold_hourly_  gold_zone_     gold_driver_
  trip_summary   demand        performance    economics
```

**Materializations:**

| Layer | Materialization | Reasoning |
|---|---|---|
| Bronze | `table` | Full rebuild on each run; data freshness is critical |
| Silver | `table` | Full rebuild ensures filter logic always applied cleanly |
| Gold | `table` | Pre-aggregated for fast BI query performance |

An alternative for large datasets would be to use `incremental` materialization on silver
with `unique_key = 'trip_id'` and `merge` strategy. This is documented in the
"Extending the Project" section below.

---

## Airflow DAG Descriptions

### DAG 1: `nyc_taxi_ingestion` (schedule: `@daily`)

Orchestrates the end-to-end raw data ingestion pipeline.

```
check_api_health
       │
       ▼
download_taxi_data
       │
       ▼
upload_to_snowflake_stage
       │
       ▼
load_to_raw_table
       │
       ▼
validate_row_count (assert >= 100,000 rows)
       │
       ▼
trigger_dbt_bronze
       │
       ▼
notify_success
```

**Key behaviors:**
- Downloads the previous month's parquet file (TLC publishes data one month behind)
- Uses SnowflakeHook PUT for reliable staged uploads with compression
- COPY INTO uses `MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE` for schema flexibility
- Row count validation provides a guardrail against truncated downloads

### DAG 2: `nyc_taxi_dbt_transform` (schedule: `@daily`)

Runs the complete dbt transformation stack after ingestion completes.

```
wait_for_ingestion (ExternalTaskSensor)
       │
       ▼
dbt_test_bronze
       │
       ▼
dbt_run_silver
       │
       ▼
dbt_test_silver
       │
       ▼
dbt_run_gold
       │
       ▼
dbt_test_gold
       │
       ▼
dbt_generate_docs
       │
       ▼
notify_transform_complete
```

**Key behaviors:**
- ExternalTaskSensor with `mode='reschedule'` (non-blocking; frees worker slots)
- Tests run between each layer — gold is only built if silver tests pass
- `dbt docs generate` keeps the data catalog current after every successful run

### DAG 3: `nyc_taxi_data_quality` (schedule: `@weekly`)

Monitors data health across all layers independently of the transformation pipeline.

```
check_bronze_freshness ─┐
check_silver_completeness ─┼──▶ generate_quality_report ──▶ alert_on_failures
check_gold_aggregations ─┘
```

**Key behaviors:**
- Checks run in parallel (no inter-dependencies) for efficiency
- Quality report is a structured JSON document for programmatic consumption
- `alert_on_failures` logs PASS/FAIL per check — extend with Slack/PagerDuty hooks

---

## Data Quality Strategy

Quality is enforced at three levels:

### Level 1: dbt Schema Tests (schema.yml)

Applied to every model in every layer:

| Test Type | Where Applied | Example |
|---|---|---|
| `not_null` | All primary keys, timestamps, amounts | `trip_id`, `pickup_datetime` |
| `unique` | All primary keys | `trip_id`, `zone_id` |
| `accepted_values` | Categorical columns | `payment_type_desc`, `borough` |
| `relationships` | Foreign key references | `pickup_location_id` → `silver_taxi_zones.zone_id` |
| `dbt_utils.expression_is_true` | Numeric constraints | `trip_duration_minutes > 0` |

### Level 2: dbt Singular Tests (tests/*.sql)

Custom SQL tests for complex business assertions:

| Test | Assertion |
|---|---|
| `assert_silver_trips_no_future_dates` | No trip has a pickup date in the future |
| `assert_gold_revenue_positive` | All gold revenue aggregates > 0 |
| `assert_trip_duration_reasonable` | No trip exceeds 300 minutes (configurable via dbt var) |

### Level 3: Airflow Data Quality DAG

Operational monitoring that runs independently of the transformation pipeline:

| Check | Threshold | Action on Failure |
|---|---|---|
| Bronze freshness | Data must be < 48 hours old | FAIL status in report |
| Silver completeness | < 5% nulls on key columns | FAIL status per column |
| Gold aggregations | Revenue > 0, trips >= 1 | FAIL status in report |

### Level 4: Business Rules in Silver SQL

Filtering logic in `silver_taxi_trips.sql`:

```sql
WHERE trip_distance > 0
  AND fare_amount > 0
  AND passenger_count BETWEEN 1 AND 6
  AND pickup_datetime < dropoff_datetime
```

---

## Extending the Project

### Adding New Data Sources

1. Add source table to `docs/snowflake_setup.sql`
2. Create `bronze_<source>.sql` model with `{{ source('raw_data', '<table>') }}`
3. Add source and model definition to `models/bronze/schema.yml`
4. Create corresponding silver and gold models following existing patterns

### Switching to Incremental Models

For very large datasets (>500M rows), switch silver to incremental:

```sql
{{
    config(
        materialized='incremental',
        unique_key='trip_id',
        incremental_strategy='merge'
    )
}}

SELECT ... FROM {{ ref('bronze_taxi_trips') }}
{% if is_incremental() %}
WHERE tpep_pickup_datetime > (SELECT MAX(pickup_datetime) FROM {{ this }})
{% endif %}
```

### Adding Green Cab or FHV Data

The schema supports multiple vendor datasets:
1. Create `raw_data.green_taxi_trips` and `raw_data.fhv_trips` tables in Snowflake
2. Add new bronze models: `bronze_green_trips.sql`, `bronze_fhv_trips.sql`
3. Create a unified silver model using `UNION ALL` across all trip sources
4. Gold models will automatically pick up the additional data

### Adding a Serving Layer for ML

1. Create a new schema: `CREATE SCHEMA nyc_taxi_lakehouse.features`
2. Add dbt models under `models/features/` with `+schema: features`
3. Feature models can reference any gold model via `{{ ref('gold_zone_performance') }}`
4. Register features in a feature store (Feast, Tecton, or Snowflake Feature Store)

### Connecting a BI Tool

**Tableau / Power BI:**
- Connect to Snowflake GOLD schema directly
- Use `nyc_taxi_role` credentials with read-only grants
- Gold tables have descriptive column names ready for dashboard labels

**Looker / Metabase:**
- Connect to Snowflake
- All gold models have clear grain documentation in schema.yml
- Use `gold_daily_trip_summary` as the primary fact table for most dashboards

### Adding dbt Snapshots

To track slowly-changing dimensions (e.g., zone name changes):

```sql
-- snapshots/snapshot_taxi_zones.sql
{% snapshot snapshot_taxi_zones %}
{{
    config(
        target_schema='snapshots',
        unique_key='zone_id',
        strategy='check',
        check_cols=['zone_name', 'borough', 'service_zone']
    )
}}
SELECT * FROM {{ ref('silver_taxi_zones') }}
{% endsnapshot %}
```

### Monitoring and Alerting

Extend `dag_data_quality.py`'s `_alert_on_failures` function to send real alerts:

```python
# Slack example
if overall_status == "FAIL":
    SlackWebhookHook(
        slack_webhook_conn_id='slack_webhook'
    ).send_text(f"Data quality FAILED: {failed_checks}")
```
