-- =============================================================================
-- NYC Taxi Lakehouse - Snowflake Environment Setup Script
-- =============================================================================
-- Description : Complete Snowflake infrastructure setup for the NYC Taxi
--               Modern Lakehouse project. Run this script as ACCOUNTADMIN
--               or SYSADMIN with sufficient privileges.
-- Author      : Data Engineering Team
-- Run order   : Execute sections top-to-bottom in a single Snowflake worksheet.
-- =============================================================================


-- =============================================================================
-- SECTION 1: Virtual Warehouse
-- =============================================================================

USE ROLE SYSADMIN;

CREATE WAREHOUSE IF NOT EXISTS nyc_taxi_wh
    WAREHOUSE_SIZE = 'X-SMALL'
    AUTO_SUSPEND   = 300          -- Auto-suspend after 5 minutes of inactivity
    AUTO_RESUME    = TRUE
    INITIALLY_SUSPENDED = TRUE
    COMMENT = 'Warehouse for NYC Taxi Lakehouse dbt and Airflow workloads';

ALTER WAREHOUSE nyc_taxi_wh SET
    MAX_CLUSTER_COUNT = 1
    MIN_CLUSTER_COUNT = 1
    SCALING_POLICY    = 'ECONOMY';


-- =============================================================================
-- SECTION 2: Database
-- =============================================================================

CREATE DATABASE IF NOT EXISTS nyc_taxi_lakehouse
    COMMENT = 'NYC Taxi Modern Lakehouse - Medallion Architecture (Bronze/Silver/Gold)';

USE DATABASE nyc_taxi_lakehouse;


-- =============================================================================
-- SECTION 3: Schemas (Medallion Layers)
-- =============================================================================

-- Raw ingestion layer: data lands here from Airflow COPY INTO
CREATE SCHEMA IF NOT EXISTS nyc_taxi_lakehouse.raw_data
    COMMENT = 'Raw ingestion layer: data loaded directly from NYC TLC parquet files';

-- Bronze layer: dbt models that select from raw with metadata columns
CREATE SCHEMA IF NOT EXISTS nyc_taxi_lakehouse.bronze
    COMMENT = 'Bronze layer: raw data surfaced via dbt models with ingestion metadata';

-- Silver layer: cleaned, typed, and business-rules-applied models
CREATE SCHEMA IF NOT EXISTS nyc_taxi_lakehouse.silver
    COMMENT = 'Silver layer: cleaned and enriched data with business logic applied';

-- Gold layer: analytics-ready aggregates and business metrics
CREATE SCHEMA IF NOT EXISTS nyc_taxi_lakehouse.gold
    COMMENT = 'Gold layer: analytics-ready aggregates and KPI models';


-- =============================================================================
-- SECTION 4: Role & User Setup
-- =============================================================================

USE ROLE SECURITYADMIN;

-- Create a dedicated role for the lakehouse project
CREATE ROLE IF NOT EXISTS nyc_taxi_role
    COMMENT = 'Role for NYC Taxi Lakehouse ETL, dbt, and Airflow operations';

-- Grant warehouse usage
GRANT USAGE ON WAREHOUSE nyc_taxi_wh TO ROLE nyc_taxi_role;

-- Grant database privileges
GRANT USAGE ON DATABASE nyc_taxi_lakehouse TO ROLE nyc_taxi_role;

-- Grant schema-level privileges on all schemas
GRANT USAGE, CREATE TABLE, CREATE VIEW, CREATE STAGE, CREATE FILE FORMAT
    ON SCHEMA nyc_taxi_lakehouse.raw_data TO ROLE nyc_taxi_role;

GRANT USAGE, CREATE TABLE, CREATE VIEW
    ON SCHEMA nyc_taxi_lakehouse.bronze TO ROLE nyc_taxi_role;

GRANT USAGE, CREATE TABLE, CREATE VIEW
    ON SCHEMA nyc_taxi_lakehouse.silver TO ROLE nyc_taxi_role;

GRANT USAGE, CREATE TABLE, CREATE VIEW
    ON SCHEMA nyc_taxi_lakehouse.gold TO ROLE nyc_taxi_role;

-- Grant table-level privileges (current and future tables)
GRANT SELECT, INSERT, UPDATE, DELETE, TRUNCATE
    ON ALL TABLES IN SCHEMA nyc_taxi_lakehouse.raw_data TO ROLE nyc_taxi_role;

GRANT SELECT, INSERT, UPDATE, DELETE, TRUNCATE
    ON FUTURE TABLES IN SCHEMA nyc_taxi_lakehouse.raw_data TO ROLE nyc_taxi_role;

GRANT SELECT, INSERT, UPDATE, DELETE, TRUNCATE
    ON ALL TABLES IN SCHEMA nyc_taxi_lakehouse.bronze TO ROLE nyc_taxi_role;

GRANT SELECT, INSERT, UPDATE, DELETE, TRUNCATE
    ON FUTURE TABLES IN SCHEMA nyc_taxi_lakehouse.bronze TO ROLE nyc_taxi_role;

GRANT SELECT, INSERT, UPDATE, DELETE, TRUNCATE
    ON ALL TABLES IN SCHEMA nyc_taxi_lakehouse.silver TO ROLE nyc_taxi_role;

GRANT SELECT, INSERT, UPDATE, DELETE, TRUNCATE
    ON FUTURE TABLES IN SCHEMA nyc_taxi_lakehouse.silver TO ROLE nyc_taxi_role;

GRANT SELECT, INSERT, UPDATE, DELETE, TRUNCATE
    ON ALL TABLES IN SCHEMA nyc_taxi_lakehouse.gold TO ROLE nyc_taxi_role;

GRANT SELECT, INSERT, UPDATE, DELETE, TRUNCATE
    ON FUTURE TABLES IN SCHEMA nyc_taxi_lakehouse.gold TO ROLE nyc_taxi_role;

-- Create the service account user
CREATE USER IF NOT EXISTS nyc_taxi_user
    PASSWORD            = 'ChangeMe_Secure_Password_123!'  -- CHANGE THIS BEFORE USE
    LOGIN_NAME          = 'nyc_taxi_user'
    DISPLAY_NAME        = 'NYC Taxi Lakehouse Service Account'
    DEFAULT_ROLE        = nyc_taxi_role
    DEFAULT_WAREHOUSE   = nyc_taxi_wh
    DEFAULT_NAMESPACE   = 'nyc_taxi_lakehouse.raw_data'
    MUST_CHANGE_PASSWORD = FALSE
    COMMENT             = 'Service account for Airflow ingestion and dbt transformations';

-- Grant role to user
GRANT ROLE nyc_taxi_role TO USER nyc_taxi_user;


-- =============================================================================
-- SECTION 5: Raw Data Tables
-- =============================================================================

USE ROLE nyc_taxi_role;
USE WAREHOUSE nyc_taxi_wh;
USE DATABASE nyc_taxi_lakehouse;
USE SCHEMA raw_data;

-- NYC Yellow Taxi Trip Records
-- Schema based on NYC TLC data dictionary for yellow taxi trips
CREATE TABLE IF NOT EXISTS nyc_taxi_lakehouse.raw_data.nyc_taxi_trips (
    vendorid                INTEGER         COMMENT 'TPEP provider code. 1=Creative Mobile Technologies, 2=VeriFone',
    tpep_pickup_datetime    TIMESTAMP_NTZ   COMMENT 'Pickup timestamp when meter was engaged',
    tpep_dropoff_datetime   TIMESTAMP_NTZ   COMMENT 'Dropoff timestamp when meter was disengaged',
    passenger_count         INTEGER         COMMENT 'Number of passengers (driver-entered)',
    trip_distance           NUMERIC(10, 2)  COMMENT 'Trip distance in miles reported by taximeter',
    ratecodeid              INTEGER         COMMENT 'Rate code: 1=Standard, 2=JFK, 3=Newark, 4=Nassau/Westchester, 5=Negotiated, 6=Group',
    store_and_fwd_flag      VARCHAR(1)      COMMENT 'Y=stored in vehicle memory before sending, N=not stored',
    pulocationid            INTEGER         COMMENT 'TLC Taxi Zone ID for pickup location',
    dolocationid            INTEGER         COMMENT 'TLC Taxi Zone ID for dropoff location',
    payment_type            INTEGER         COMMENT '1=Credit card, 2=Cash, 3=No charge, 4=Dispute, 5=Unknown, 6=Voided',
    fare_amount             NUMERIC(10, 2)  COMMENT 'Time-and-distance fare from the meter',
    extra                   NUMERIC(10, 2)  COMMENT 'Miscellaneous extras (rush hour/overnight surcharges)',
    mta_tax                 NUMERIC(10, 2)  COMMENT '$0.50 MTA tax triggered on metered rate',
    tip_amount              NUMERIC(10, 2)  COMMENT 'Tip amount (auto-populated for credit card; cash tips excluded)',
    tolls_amount            NUMERIC(10, 2)  COMMENT 'Total amount of all tolls paid in trip',
    improvement_surcharge   NUMERIC(10, 2)  COMMENT '$0.30 improvement surcharge at flag drop',
    total_amount            NUMERIC(10, 2)  COMMENT 'Total amount charged (excludes cash tips)',
    congestion_surcharge    NUMERIC(10, 2)  COMMENT 'NYS congestion surcharge',
    airport_fee             NUMERIC(10, 2)  COMMENT '$1.25 pickup fee at LGA and JFK airports'
)
COMMENT = 'Raw NYC Yellow Taxi trip records loaded from monthly TLC parquet files';

-- NYC Taxi Zone Lookup Table
-- Maps TLC LocationID to human-readable borough/zone names
CREATE TABLE IF NOT EXISTS nyc_taxi_lakehouse.raw_data.nyc_taxi_zones (
    locationid    INTEGER      NOT NULL   COMMENT 'Unique TLC Taxi Zone identifier',
    borough       VARCHAR(50)             COMMENT 'NYC borough name',
    zone          VARCHAR(100)            COMMENT 'Zone name within the borough',
    service_zone  VARCHAR(50)             COMMENT 'Service zone classification (Yellow Zone, Boro Zone, Airports, EWR)'
)
COMMENT = 'NYC Taxi Zone lookup table mapping LocationID to borough and zone name';


-- =============================================================================
-- SECTION 6: Internal Stage for Parquet Files
-- =============================================================================

USE SCHEMA raw_data;

-- Internal stage for Airflow to PUT parquet files before COPY INTO
CREATE STAGE IF NOT EXISTS nyc_taxi_lakehouse.raw_data.taxi_stage
    COMMENT = 'Internal Snowflake stage for NYC TLC parquet files uploaded by Airflow';

-- List stage contents (run after files are uploaded)
-- LIST @nyc_taxi_lakehouse.raw_data.taxi_stage;


-- =============================================================================
-- SECTION 7: File Format for Parquet
-- =============================================================================

CREATE FILE FORMAT IF NOT EXISTS nyc_taxi_lakehouse.raw_data.parquet_format
    TYPE                = 'PARQUET'
    SNAPPY_COMPRESSION  = TRUE
    COMMENT             = 'Parquet file format with Snappy compression for NYC TLC data';


-- =============================================================================
-- SECTION 8: Verification Queries
-- =============================================================================

-- Verify objects were created
SHOW WAREHOUSES LIKE 'nyc_taxi_wh';
SHOW DATABASES LIKE 'nyc_taxi_lakehouse';
SHOW SCHEMAS IN DATABASE nyc_taxi_lakehouse;
SHOW TABLES IN SCHEMA nyc_taxi_lakehouse.raw_data;
SHOW STAGES IN SCHEMA nyc_taxi_lakehouse.raw_data;
SHOW FILE FORMATS IN SCHEMA nyc_taxi_lakehouse.raw_data;

-- Verify role grants
SHOW GRANTS TO ROLE nyc_taxi_role;
SHOW GRANTS TO USER nyc_taxi_user;

-- =============================================================================
-- END OF SETUP SCRIPT
-- =============================================================================
-- Next steps:
--   1. Change the nyc_taxi_user password above before executing in production
--   2. Run this script as ACCOUNTADMIN or SYSADMIN
--   3. Update your .env file with the account identifier, user, and password
--   4. Test connectivity: snowsql -a <account> -u nyc_taxi_user
--   5. Run: dbt debug --profiles-dir dbt_project/ to verify dbt connection
-- =============================================================================
