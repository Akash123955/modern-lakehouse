/*
    Model: bronze_taxi_trips
    Layer: Bronze (Raw Ingestion)
    Description: Raw NYC Yellow Taxi trip records ingested from the NYC TLC API.
                 Minimal transformation - just adds metadata columns for lineage tracking.
                 Source: RAW_DATA.NYC_TAXI_TRIPS (loaded via Airflow COPY INTO from Parquet stage)
*/

{{
    config(
        materialized='table',
        schema='bronze',
        tags=['bronze', 'nyc_taxi', 'trips'],
        comment='Raw NYC Yellow Taxi trip records with ingestion metadata'
    )
}}

SELECT
    -- Original source columns (preserved as-is from raw ingest)
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

    -- Metadata columns for data lineage and freshness tracking
    CURRENT_TIMESTAMP()                          AS _loaded_at,
    'nyc_taxi_api'                               AS _source,
    'yellow_tripdata'                            AS _file_name

FROM {{ source('raw_data', 'nyc_taxi_trips') }}
