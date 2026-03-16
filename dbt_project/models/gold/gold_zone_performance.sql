/*
    Model: gold_zone_performance
    Layer: Gold (Analytics-Ready Aggregates)
    Description: Zone-level performance metrics for NYC Taxi operations.
                 Calculates pickup/dropoff volumes, average fares, most common
                 destinations, revenue rankings within borough, and recent trip counts.
                 Useful for identifying high-value zones and supply/demand imbalances.

    Grain: One row per zone_id
*/

{{
    config(
        materialized='table',
        schema='gold',
        tags=['gold', 'nyc_taxi', 'zones', 'performance', 'analytics'],
        comment='Zone-level NYC Taxi performance metrics with ranking and destination analysis'
    )
}}

WITH trips AS (

    SELECT
        trip_id,
        pickup_location_id,
        dropoff_location_id,
        fare_amount,
        total_amount,
        pickup_datetime

    FROM {{ ref('silver_taxi_trips') }}

),

zones AS (

    SELECT
        zone_id,
        zone_name,
        borough,
        borough_code,
        is_airport

    FROM {{ ref('silver_taxi_zones') }}

),

-- Aggregate pickup stats per zone
pickup_stats AS (

    SELECT
        t.pickup_location_id                        AS zone_id,
        COUNT(t.trip_id)                            AS total_pickups,
        ROUND(AVG(t.fare_amount), 2)                AS avg_fare_from_zone,
        ROUND(SUM(t.total_amount), 2)               AS total_revenue_from_zone,

        -- Recent 30-day trip count
        SUM(
            CASE
                WHEN t.pickup_datetime >= DATEADD(
                    'day', -30, CURRENT_DATE()
                )
                THEN 1
                ELSE 0
            END
        )                                           AS trip_count_last_30_days

    FROM trips t
    GROUP BY
        t.pickup_location_id

),

-- Aggregate dropoff stats per zone
dropoff_stats AS (

    SELECT
        t.dropoff_location_id                       AS zone_id,
        COUNT(t.trip_id)                            AS total_dropoffs,
        ROUND(AVG(t.fare_amount), 2)                AS avg_fare_to_zone

    FROM trips t
    GROUP BY
        t.dropoff_location_id

),

-- Determine the most common destination for each pickup zone using window functions
destination_ranked AS (

    SELECT
        pickup_location_id,
        dropoff_location_id,
        COUNT(trip_id)                              AS route_count,
        ROW_NUMBER() OVER (
            PARTITION BY pickup_location_id
            ORDER BY COUNT(trip_id) DESC
        )                                           AS destination_rank

    FROM trips
    GROUP BY
        pickup_location_id,
        dropoff_location_id

),

most_common_destination AS (

    SELECT
        dr.pickup_location_id                       AS zone_id,
        z.zone_name                                 AS most_common_destination,
        dr.route_count                              AS most_common_destination_trips

    FROM destination_ranked dr
    LEFT JOIN zones z
        ON dr.dropoff_location_id = z.zone_id
    WHERE dr.destination_rank = 1

),

-- Combine all zone metrics
zone_metrics AS (

    SELECT
        z.zone_id,
        z.zone_name,
        z.borough,
        z.borough_code,
        z.is_airport,

        COALESCE(ps.total_pickups, 0)               AS total_pickups,
        COALESCE(ds.total_dropoffs, 0)              AS total_dropoffs,
        ps.avg_fare_from_zone,
        ds.avg_fare_to_zone,
        COALESCE(ps.total_revenue_from_zone, 0)     AS total_revenue_from_zone,
        COALESCE(ps.trip_count_last_30_days, 0)     AS trip_count_last_30_days,

        mcd.most_common_destination,
        COALESCE(mcd.most_common_destination_trips, 0) AS most_common_destination_trips

    FROM zones z
    LEFT JOIN pickup_stats ps
        ON z.zone_id = ps.zone_id
    LEFT JOIN dropoff_stats ds
        ON z.zone_id = ds.zone_id
    LEFT JOIN most_common_destination mcd
        ON z.zone_id = mcd.zone_id

)

SELECT
    zone_id,
    zone_name,
    borough,
    borough_code,
    is_airport,
    total_pickups,
    total_dropoffs,
    avg_fare_from_zone,
    avg_fare_to_zone,
    total_revenue_from_zone,
    trip_count_last_30_days,
    most_common_destination,
    most_common_destination_trips,

    -- Revenue rank within borough (1 = highest revenue zone in that borough)
    RANK() OVER (
        PARTITION BY borough
        ORDER BY total_revenue_from_zone DESC
    )                                               AS revenue_rank,

    -- Overall percentile rank by pickup volume
    ROUND(
        PERCENT_RANK() OVER (
            ORDER BY total_pickups
        ) * 100.0,
        1
    )                                               AS pickup_volume_percentile

FROM zone_metrics
ORDER BY
    borough_code,
    total_pickups DESC
