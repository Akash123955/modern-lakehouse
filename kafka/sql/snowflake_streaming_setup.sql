-- =============================================================================
-- Snowflake Streaming Setup for Real-Time NYC Taxi Trip Ingestion
-- =============================================================================
-- Run this script once to set up the Snowpipe infrastructure.
-- This creates a separate streaming landing table and pipe alongside the
-- existing batch pipeline — both feed into the same bronze → silver → gold
-- medallion architecture.
--
-- Architecture:
--   Kafka Producer → Kafka Topic → Snowpipe Consumer (Python)
--       → Snowflake Stage → Snowpipe → raw_data.nyc_taxi_trips_streaming
--       → bronze_taxi_trips_streaming → (merges into silver_taxi_trips)
-- =============================================================================

USE ROLE nyc_taxi_role;
USE DATABASE nyc_taxi_lakehouse;
USE SCHEMA raw_data;
USE WAREHOUSE nyc_taxi_wh;

-- ---------------------------------------------------------------------------
-- 1. Streaming landing table (JSON micro-batches from Kafka consumer)
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS raw_data.nyc_taxi_trips_streaming (
    event_id           VARCHAR(64)     NOT NULL,   -- UUID assigned by producer
    vendorid           INTEGER,
    tpep_pickup_datetime  TIMESTAMP_NTZ,
    tpep_dropoff_datetime TIMESTAMP_NTZ,
    passenger_count    INTEGER,
    trip_distance      NUMERIC(10, 2),
    ratecodeid         INTEGER,
    store_and_fwd_flag VARCHAR(1),
    pulocationid       INTEGER,
    dolocationid       INTEGER,
    payment_type       INTEGER,
    fare_amount        NUMERIC(10, 2),
    extra              NUMERIC(10, 2),
    mta_tax            NUMERIC(10, 2),
    tip_amount         NUMERIC(10, 2),
    tolls_amount       NUMERIC(10, 2),
    improvement_surcharge NUMERIC(10, 2),
    total_amount       NUMERIC(10, 2),
    congestion_surcharge  NUMERIC(10, 2),
    airport_fee        NUMERIC(10, 2),
    -- Streaming-specific metadata
    kafka_topic        VARCHAR(128),
    kafka_partition    INTEGER,
    kafka_offset       BIGINT,
    kafka_timestamp    TIMESTAMP_NTZ,
    _ingested_at       TIMESTAMP_NTZ  DEFAULT CURRENT_TIMESTAMP(),
    _source            VARCHAR(32)    DEFAULT 'kafka_streaming'
);

COMMENT ON TABLE raw_data.nyc_taxi_trips_streaming IS
    'Real-time NYC taxi trip events ingested via Kafka + Snowpipe. '
    'Complements the daily batch pipeline in raw_data.nyc_taxi_trips.';

-- ---------------------------------------------------------------------------
-- 2. Internal stage for Kafka consumer to PUT JSON micro-batch files
-- ---------------------------------------------------------------------------
CREATE STAGE IF NOT EXISTS raw_data.taxi_streaming_stage
    FILE_FORMAT = (
        TYPE = 'JSON'
        STRIP_OUTER_ARRAY = TRUE
        STRIP_NULL_VALUES = FALSE
        IGNORE_UTF8_ERRORS = TRUE
    )
    COMMENT = 'Internal stage for Snowpipe streaming ingestion from Kafka consumer';

-- ---------------------------------------------------------------------------
-- 3. Snowpipe — auto-ingest pipe (triggered programmatically via REST API)
-- ---------------------------------------------------------------------------
CREATE PIPE IF NOT EXISTS raw_data.taxi_streaming_pipe
    AUTO_INGEST = FALSE
    COMMENT = 'Snowpipe for real-time NYC taxi trip event ingestion from Kafka'
AS
COPY INTO raw_data.nyc_taxi_trips_streaming (
    event_id,
    vendorid,
    tpep_pickup_datetime,
    tpep_dropoff_datetime,
    passenger_count,
    trip_distance,
    ratecodeid,
    store_and_fwd_flag,
    pulocationid,
    dolocationid,
    payment_type,
    fare_amount,
    extra,
    mta_tax,
    tip_amount,
    tolls_amount,
    improvement_surcharge,
    total_amount,
    congestion_surcharge,
    airport_fee,
    kafka_topic,
    kafka_partition,
    kafka_offset,
    kafka_timestamp
)
FROM (
    SELECT
        $1:event_id::VARCHAR(64),
        $1:vendorid::INTEGER,
        $1:tpep_pickup_datetime::TIMESTAMP_NTZ,
        $1:tpep_dropoff_datetime::TIMESTAMP_NTZ,
        $1:passenger_count::INTEGER,
        $1:trip_distance::NUMERIC(10, 2),
        $1:ratecodeid::INTEGER,
        $1:store_and_fwd_flag::VARCHAR(1),
        $1:pulocationid::INTEGER,
        $1:dolocationid::INTEGER,
        $1:payment_type::INTEGER,
        $1:fare_amount::NUMERIC(10, 2),
        $1:extra::NUMERIC(10, 2),
        $1:mta_tax::NUMERIC(10, 2),
        $1:tip_amount::NUMERIC(10, 2),
        $1:tolls_amount::NUMERIC(10, 2),
        $1:improvement_surcharge::NUMERIC(10, 2),
        $1:total_amount::NUMERIC(10, 2),
        $1:congestion_surcharge::NUMERIC(10, 2),
        $1:airport_fee::NUMERIC(10, 2),
        $1:kafka_metadata:topic::VARCHAR(128),
        $1:kafka_metadata:partition::INTEGER,
        $1:kafka_metadata:offset::BIGINT,
        $1:kafka_metadata:timestamp::TIMESTAMP_NTZ
    FROM @raw_data.taxi_streaming_stage
);

-- ---------------------------------------------------------------------------
-- 4. Grant the Snowpipe service principal access to the pipe
-- ---------------------------------------------------------------------------
GRANT OPERATE ON PIPE raw_data.taxi_streaming_pipe TO ROLE nyc_taxi_role;
GRANT READ ON STAGE raw_data.taxi_streaming_stage TO ROLE nyc_taxi_role;
GRANT WRITE ON STAGE raw_data.taxi_streaming_stage TO ROLE nyc_taxi_role;

-- ---------------------------------------------------------------------------
-- 5. Monitoring view — Snowpipe ingestion lag & throughput
-- ---------------------------------------------------------------------------
CREATE OR REPLACE VIEW raw_data.v_snowpipe_ingestion_health AS
SELECT
    pipe_name,
    file_name,
    file_size,
    row_count,
    row_parsed,
    first_error_message,
    first_error_line,
    first_error_character,
    status,
    last_load_time,
    DATEDIFF('second', last_load_time, CURRENT_TIMESTAMP()) AS seconds_since_last_load,
    CASE
        WHEN DATEDIFF('minute', last_load_time, CURRENT_TIMESTAMP()) <= 2  THEN 'HEALTHY'
        WHEN DATEDIFF('minute', last_load_time, CURRENT_TIMESTAMP()) <= 5  THEN 'WARNING'
        ELSE 'CRITICAL'
    END AS health_status
FROM TABLE(
    INFORMATION_SCHEMA.COPY_HISTORY(
        TABLE_NAME     => 'NYC_TAXI_TRIPS_STREAMING',
        START_TIME     => DATEADD('hour', -24, CURRENT_TIMESTAMP())
    )
)
ORDER BY last_load_time DESC;

COMMENT ON VIEW raw_data.v_snowpipe_ingestion_health IS
    'Real-time Snowpipe health monitoring. health_status: HEALTHY=<2min, WARNING=<5min, CRITICAL=>5min lag';

-- ---------------------------------------------------------------------------
-- 6. Verify setup
-- ---------------------------------------------------------------------------
SHOW PIPES IN SCHEMA raw_data;
SHOW STAGES IN SCHEMA raw_data;

SELECT 'Streaming setup complete. Pipe: taxi_streaming_pipe | Stage: taxi_streaming_stage' AS status;
