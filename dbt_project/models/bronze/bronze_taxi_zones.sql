/*
    Model: bronze_taxi_zones
    Layer: Bronze (Raw Ingestion)
    Description: Raw NYC Taxi Zone lookup table. Maps LocationID to borough, zone name,
                 and service zone. Used to enrich trip data with geographic context.
                 Source: RAW_DATA.NYC_TAXI_ZONES
*/

{{
    config(
        materialized='table',
        schema='bronze',
        tags=['bronze', 'nyc_taxi', 'zones', 'reference'],
        comment='Raw NYC Taxi Zone lookup data with ingestion metadata'
    )
}}

SELECT
    -- Original source columns (preserved as-is)
    locationid,
    borough,
    zone,
    service_zone,

    -- Metadata columns for data lineage and freshness tracking
    CURRENT_TIMESTAMP()                          AS _loaded_at,
    'nyc_taxi_api'                               AS _source,
    'yellow_tripdata'                            AS _file_name

FROM {{ source('raw_data', 'nyc_taxi_zones') }}
