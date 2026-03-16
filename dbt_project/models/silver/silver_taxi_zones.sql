/*
    Model: silver_taxi_zones
    Layer: Silver (Cleaned & Typed)
    Description: Cleaned NYC Taxi Zone lookup table. Trims whitespace from string
                 columns, adds a numeric borough_code for easier joining, and
                 standardizes column naming conventions.
*/

{{
    config(
        materialized='table',
        schema='silver',
        tags=['silver', 'nyc_taxi', 'zones', 'reference', 'cleaned'],
        comment='Cleaned NYC Taxi Zone reference data with standardized naming and borough codes'
    )
}}

WITH source AS (

    SELECT * FROM {{ ref('bronze_taxi_zones') }}

),

cleaned AS (

    SELECT
        -- Primary key
        CAST(locationid AS INTEGER)              AS zone_id,

        -- Cleaned string columns (trim whitespace, standardize case)
        {{ clean_string('borough') }}            AS borough,
        {{ clean_string('zone') }}               AS zone_name,
        {{ clean_string('service_zone') }}       AS service_zone,

        -- Numeric borough code for efficient joining
        CASE TRIM(UPPER(borough))
            WHEN 'MANHATTAN'     THEN 1
            WHEN 'BRONX'         THEN 2
            WHEN 'BROOKLYN'      THEN 3
            WHEN 'QUEENS'        THEN 4
            WHEN 'STATEN ISLAND' THEN 5
            WHEN 'EWR'           THEN 6
            ELSE 0
        END                                      AS borough_code,

        -- Is this location an airport?
        CASE
            WHEN TRIM(UPPER(service_zone)) = 'AIRPORTS' THEN TRUE
            ELSE FALSE
        END                                      AS is_airport,

        -- Metadata passthrough
        _loaded_at,
        _source,
        _file_name

    FROM source

)

SELECT * FROM cleaned
