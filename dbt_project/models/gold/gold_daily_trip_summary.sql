/*
    Model: gold_daily_trip_summary
    Layer: Gold (Analytics-Ready Aggregates)
    Description: Daily trip summary aggregated by date and pickup borough.
                 Provides executive-level KPIs for operational dashboards and
                 revenue reporting. Joins to zone data for borough-level grouping.

    Grain: One row per (trip_date, pickup_borough)
*/

{{
    config(
        materialized='table',
        schema='gold',
        tags=['gold', 'nyc_taxi', 'daily', 'summary', 'analytics'],
        comment='Daily NYC Taxi trip KPIs aggregated by date and pickup borough'
    )
}}

WITH trips AS (

    SELECT
        t.trip_id,
        CAST(t.pickup_datetime AS DATE)          AS trip_date,
        t.pickup_location_id,
        t.dropoff_location_id,
        t.passenger_count,
        t.trip_distance_miles,
        t.fare_amount,
        t.tip_amount,
        t.total_amount,
        t.trip_duration_minutes,
        t.trip_speed_mph,
        t.is_airport_trip,
        t.payment_type_desc

    FROM {{ ref('silver_taxi_trips') }} t

),

zones AS (

    SELECT
        zone_id,
        borough

    FROM {{ ref('silver_taxi_zones') }}

),

trips_with_borough AS (

    SELECT
        tr.trip_id,
        tr.trip_date,
        tr.passenger_count,
        tr.trip_distance_miles,
        tr.fare_amount,
        tr.tip_amount,
        tr.total_amount,
        tr.trip_duration_minutes,
        tr.trip_speed_mph,
        tr.is_airport_trip,
        tr.payment_type_desc,
        COALESCE(pu.borough, 'Unknown')          AS pickup_borough

    FROM trips tr
    LEFT JOIN zones pu
        ON tr.pickup_location_id = pu.zone_id

),

daily_aggregates AS (

    SELECT
        trip_date,
        pickup_borough,

        -- Volume metrics
        COUNT(trip_id)                           AS total_trips,
        SUM(passenger_count)                     AS total_passengers,

        -- Revenue metrics
        ROUND(SUM(total_amount), 2)              AS total_revenue,
        ROUND(AVG(fare_amount), 2)               AS avg_fare_amount,
        ROUND(AVG(tip_amount), 2)                AS avg_tip_amount,

        -- Trip characteristic metrics
        ROUND(AVG(trip_distance_miles), 2)       AS avg_trip_distance,
        ROUND(AVG(trip_duration_minutes), 2)     AS avg_trip_duration_minutes,
        ROUND(AVG(trip_speed_mph), 2)            AS avg_speed_mph,

        -- Segment counts
        SUM(CASE WHEN is_airport_trip THEN 1 ELSE 0 END)          AS airport_trips_count,
        SUM(CASE WHEN payment_type_desc = 'Credit card' THEN 1 ELSE 0 END) AS credit_card_trips,
        SUM(CASE WHEN payment_type_desc = 'Cash' THEN 1 ELSE 0 END)        AS cash_trips

    FROM trips_with_borough
    GROUP BY
        trip_date,
        pickup_borough

)

SELECT
    trip_date,
    pickup_borough,
    total_trips,
    total_passengers,
    total_revenue,
    avg_fare_amount,
    avg_tip_amount,
    avg_trip_distance,
    avg_trip_duration_minutes,
    avg_speed_mph,
    airport_trips_count,
    credit_card_trips,
    cash_trips,

    -- Derived share metrics
    ROUND(
        CAST(airport_trips_count AS NUMERIC) / NULLIF(total_trips, 0) * 100.0,
        2
    )                                            AS airport_trip_pct,
    ROUND(
        CAST(credit_card_trips AS NUMERIC) / NULLIF(total_trips, 0) * 100.0,
        2
    )                                            AS credit_card_pct

FROM daily_aggregates
ORDER BY
    trip_date DESC,
    pickup_borough
