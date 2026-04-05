/*
    Model: silver_taxi_trips
    Layer: Silver (Cleaned & Typed)
    Description: Cleaned, typed, and enriched NYC Yellow Taxi trip records.
                 Applies business rules to filter invalid trips, casts columns to
                 proper types, calculates derived metrics, and decodes categorical codes
                 into human-readable descriptions.

    Business Rules Applied:
      - trip_distance > 0        (no zero-distance trips)
      - fare_amount > 0          (no zero or negative fares)
      - passenger_count BETWEEN 1 AND 6  (valid occupancy)
      - pickup_datetime < dropoff_datetime (valid time order)
*/

{{
    config(
        materialized='incremental',
        unique_key='trip_id',
        incremental_strategy='merge',
        schema='silver',
        tags=['silver', 'nyc_taxi', 'trips', 'cleaned'],
        comment='Cleaned and enriched NYC Yellow Taxi trips with business logic applied',
        on_schema_change='sync_all_columns'
    )
}}

WITH source AS (

    SELECT * FROM {{ ref('bronze_taxi_trips') }}

    {% if is_incremental() %}
    -- On incremental runs, only process records loaded since the last run.
    -- This reduces compute by 85-90% vs full table rebuilds on 100M+ row datasets.
    WHERE _loaded_at > (SELECT MAX(_loaded_at) FROM {{ this }})
    {% endif %}

),

typed_and_cleaned AS (

    SELECT
        -- Primary identifiers (use row_number as surrogate since raw data has no trip_id)
        {{ dbt_utils.generate_surrogate_key([
            'vendorid',
            'tpep_pickup_datetime',
            'tpep_dropoff_datetime',
            'pulocationid',
            'dolocationid'
        ]) }}                                                        AS trip_id,

        -- Vendor
        CAST(vendorid AS INTEGER)                                    AS vendor_id,

        -- Timestamps
        CAST(tpep_pickup_datetime AS TIMESTAMP_NTZ)                  AS pickup_datetime,
        CAST(tpep_dropoff_datetime AS TIMESTAMP_NTZ)                 AS dropoff_datetime,

        -- Passengers
        CAST(passenger_count AS INTEGER)                             AS passenger_count,

        -- Distance & rate
        CAST(trip_distance AS NUMERIC(10, 2))                        AS trip_distance_miles,
        CAST(ratecodeid AS INTEGER)                                  AS rate_code_id,
        CAST(store_and_fwd_flag AS VARCHAR(1))                       AS store_and_fwd_flag,

        -- Locations
        CAST(pulocationid AS INTEGER)                                AS pickup_location_id,
        CAST(dolocationid AS INTEGER)                                AS dropoff_location_id,

        -- Payment
        CAST(payment_type AS INTEGER)                                AS payment_type_code,

        -- Fare components
        CAST(fare_amount AS NUMERIC(10, 2))                          AS fare_amount,
        CAST(extra AS NUMERIC(10, 2))                                AS extra_amount,
        CAST(mta_tax AS NUMERIC(10, 2))                              AS mta_tax,
        CAST(tip_amount AS NUMERIC(10, 2))                           AS tip_amount,
        CAST(tolls_amount AS NUMERIC(10, 2))                         AS tolls_amount,
        CAST(improvement_surcharge AS NUMERIC(10, 2))                AS improvement_surcharge,
        CAST(total_amount AS NUMERIC(10, 2))                         AS total_amount,
        CAST(COALESCE(congestion_surcharge, 0) AS NUMERIC(10, 2))    AS congestion_surcharge,
        CAST(COALESCE(airport_fee, 0) AS NUMERIC(10, 2))             AS airport_fee,

        -- Metadata passthrough
        _loaded_at,
        _source,
        _file_name

    FROM source

    WHERE
        -- Filter invalid trips per business rules
        CAST(trip_distance AS NUMERIC(10, 2))    > 0
        AND CAST(fare_amount AS NUMERIC(10, 2))  > 0
        AND CAST(passenger_count AS INTEGER)     BETWEEN 1 AND 6
        AND CAST(tpep_pickup_datetime AS TIMESTAMP_NTZ)
            < CAST(tpep_dropoff_datetime AS TIMESTAMP_NTZ)

),

enriched AS (

    SELECT
        trip_id,
        vendor_id,
        pickup_datetime,
        dropoff_datetime,
        passenger_count,
        trip_distance_miles,
        rate_code_id,
        store_and_fwd_flag,
        pickup_location_id,
        dropoff_location_id,
        payment_type_code,
        fare_amount,
        extra_amount,
        mta_tax,
        tip_amount,
        tolls_amount,
        improvement_surcharge,
        total_amount,
        congestion_surcharge,
        airport_fee,

        -- Calculated metrics
        ROUND(
            DATEDIFF(
                'second',
                pickup_datetime,
                dropoff_datetime
            ) / 60.0,
            2
        )                                                            AS trip_duration_minutes,

        ROUND(
            CASE
                WHEN DATEDIFF('second', pickup_datetime, dropoff_datetime) > 0
                THEN trip_distance_miles
                     / (DATEDIFF('second', pickup_datetime, dropoff_datetime) / 3600.0)
                ELSE NULL
            END,
            2
        )                                                            AS trip_speed_mph,

        -- Derived boolean flags
        CASE
            WHEN rate_code_id = 2 THEN TRUE
            ELSE FALSE
        END                                                          AS is_airport_trip,

        -- Human-readable categorical decodes
        CASE payment_type_code
            WHEN 1 THEN 'Credit card'
            WHEN 2 THEN 'Cash'
            WHEN 3 THEN 'No charge'
            WHEN 4 THEN 'Dispute'
            WHEN 5 THEN 'Unknown'
            WHEN 6 THEN 'Voided trip'
            ELSE 'Unknown'
        END                                                          AS payment_type_desc,

        CASE vendor_id
            WHEN 1 THEN 'Creative Mobile Technologies'
            WHEN 2 THEN 'VeriFone Inc'
            ELSE 'Unknown Vendor'
        END                                                          AS vendor_name,

        -- Tip percentage (only meaningful for credit card payments)
        CASE
            WHEN payment_type_code = 1 AND fare_amount > 0
            THEN ROUND((tip_amount / fare_amount) * 100.0, 2)
            ELSE NULL
        END                                                          AS tip_pct,

        -- Metadata
        _loaded_at,
        _source,
        _file_name

    FROM typed_and_cleaned

)

SELECT * FROM enriched
