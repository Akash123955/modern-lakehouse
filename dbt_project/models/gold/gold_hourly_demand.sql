/*
    Model: gold_hourly_demand
    Layer: Gold (Analytics-Ready Aggregates)
    Description: Hourly demand analysis for NYC Yellow Taxi trips, broken down by
                 hour of day, day of week, and pickup borough. Uses window functions
                 to calculate the peak_hour_flag marking the top 20% busiest hours.
                 Useful for capacity planning, pricing strategy, and surge analysis.

    Grain: One row per (trip_hour, day_of_week, pickup_borough)
*/

{{
    config(
        materialized='table',
        schema='gold',
        tags=['gold', 'nyc_taxi', 'hourly', 'demand', 'analytics'],
        comment='Hourly NYC Taxi demand patterns by day of week and borough with peak hour flags'
    )
}}

WITH trips AS (

    SELECT
        t.trip_id,
        DATE_PART('hour', t.pickup_datetime)        AS trip_hour,
        DAYOFWEEK(t.pickup_datetime)                AS day_of_week_num,
        DAYNAME(t.pickup_datetime)                  AS day_of_week_name,
        t.pickup_location_id,
        t.trip_duration_minutes

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
        t.trip_id,
        t.trip_hour,
        t.day_of_week_num,
        t.day_of_week_name,
        t.pickup_location_id,
        t.trip_duration_minutes,
        COALESCE(z.borough, 'Unknown')              AS pickup_borough

    FROM trips t
    LEFT JOIN zones z
        ON t.pickup_location_id = z.zone_id

),

-- Count distinct zones per borough/hour to calculate wait efficiency
zone_counts AS (

    SELECT
        CAST(zone_id AS INTEGER)                    AS zone_id,
        borough,
        COUNT(DISTINCT zone_id) OVER (
            PARTITION BY borough
        )                                           AS zones_in_borough

    FROM {{ ref('silver_taxi_zones') }}

),

hourly_aggregates AS (

    SELECT
        trip_hour,
        day_of_week_num,
        day_of_week_name,
        pickup_borough,

        COUNT(trip_id)                              AS total_trips,
        ROUND(AVG(trip_duration_minutes), 2)        AS avg_trip_duration_minutes,

        -- Trip count per distinct zone in borough per hour (efficiency proxy)
        ROUND(
            CAST(COUNT(trip_id) AS NUMERIC) /
            NULLIF(
                MAX(zc.zones_in_borough),
                0
            ),
            2
        )                                           AS avg_wait_efficiency

    FROM trips_with_borough twb
    LEFT JOIN (
        SELECT DISTINCT borough, zones_in_borough
        FROM zone_counts
    ) zc
        ON twb.pickup_borough = zc.borough

    GROUP BY
        trip_hour,
        day_of_week_num,
        day_of_week_name,
        pickup_borough

),

ranked AS (

    SELECT
        *,
        -- Rank hours by volume within each borough and day of week
        PERCENT_RANK() OVER (
            PARTITION BY pickup_borough, day_of_week_num
            ORDER BY total_trips
        )                                           AS volume_percentile

    FROM hourly_aggregates

)

SELECT
    trip_hour,
    day_of_week_num,
    day_of_week_name,
    pickup_borough,
    total_trips,
    avg_trip_duration_minutes,
    avg_wait_efficiency,

    -- Flag top 20% hours as peak hours
    CASE
        WHEN volume_percentile >= 0.80 THEN TRUE
        ELSE FALSE
    END                                             AS peak_hour_flag,

    ROUND(volume_percentile * 100.0, 1)             AS volume_percentile_rank

FROM ranked
ORDER BY
    day_of_week_num,
    trip_hour,
    pickup_borough
