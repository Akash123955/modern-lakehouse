/*
    Model: gold_driver_economics
    Layer: Gold (Analytics-Ready Aggregates)
    Description: Driver and vendor economics model analyzing fare efficiency,
                 tipping behavior, and revenue contribution by vendor and payment type.
                 Helps identify the most economically productive vendor/payment combinations
                 and informs pricing and incentive strategies.

    Grain: One row per (vendor_name, payment_type_desc)
*/

{{
    config(
        materialized='table',
        schema='gold',
        tags=['gold', 'nyc_taxi', 'economics', 'vendor', 'analytics'],
        comment='Driver and vendor economics by vendor name and payment type'
    )
}}

WITH trips AS (

    SELECT
        vendor_name,
        payment_type_desc,
        fare_amount,
        tip_amount,
        tip_pct,
        total_amount,
        trip_distance_miles,
        trip_duration_minutes,
        pickup_datetime

    FROM {{ ref('silver_taxi_trips') }}

),

-- Calculate overall totals for revenue share computation
totals AS (

    SELECT
        SUM(total_amount)                           AS grand_total_revenue,
        COUNT(*)                                    AS grand_total_trips

    FROM trips

),

-- Estimate trips per hour by counting trips within hourly buckets
hourly_trip_counts AS (

    SELECT
        vendor_name,
        DATE_TRUNC('hour', pickup_datetime)         AS trip_hour_bucket,
        COUNT(*)                                    AS trips_in_hour

    FROM trips
    GROUP BY
        vendor_name,
        DATE_TRUNC('hour', pickup_datetime)

),

avg_hourly_by_vendor AS (

    SELECT
        vendor_name,
        ROUND(AVG(trips_in_hour), 2)                AS avg_trips_per_hour

    FROM hourly_trip_counts
    GROUP BY
        vendor_name

),

economics AS (

    SELECT
        t.vendor_name,
        t.payment_type_desc,

        -- Volume metrics
        COUNT(*)                                    AS total_trips,
        ROUND(SUM(t.total_amount), 2)               AS total_revenue,

        -- Fare efficiency metrics
        ROUND(
            SUM(t.fare_amount) / NULLIF(SUM(t.trip_distance_miles), 0),
            2
        )                                           AS avg_fare_per_mile,

        ROUND(AVG(t.fare_amount), 2)                AS avg_fare_amount,
        ROUND(AVG(t.tip_amount), 2)                 AS avg_tip_amount,
        ROUND(AVG(t.total_amount), 2)               AS avg_total_amount,

        -- Tip metrics (only meaningful for credit card)
        ROUND(AVG(
            CASE
                WHEN t.payment_type_desc = 'Credit card'
                THEN t.tip_pct
                ELSE NULL
            END
        ), 2)                                       AS avg_tip_percentage,

        -- Trip efficiency
        ROUND(AVG(t.trip_distance_miles), 2)        AS avg_trip_distance_miles,
        ROUND(AVG(t.trip_duration_minutes), 2)      AS avg_trip_duration_minutes,

        ROUND(
            SUM(t.fare_amount) / NULLIF(SUM(t.trip_duration_minutes / 60.0), 0),
            2
        )                                           AS avg_revenue_per_hour

    FROM trips t
    GROUP BY
        t.vendor_name,
        t.payment_type_desc

)

SELECT
    e.vendor_name,
    e.payment_type_desc,
    e.total_trips,
    e.total_revenue,
    e.avg_fare_per_mile,
    e.avg_fare_amount,
    e.avg_tip_amount,
    e.avg_total_amount,
    e.avg_tip_percentage,
    e.avg_trip_distance_miles,
    e.avg_trip_duration_minutes,
    e.avg_revenue_per_hour,

    -- Hourly trip rate for this vendor (across all payment types)
    COALESCE(ah.avg_trips_per_hour, 0)              AS avg_trips_per_hour,

    -- Revenue share as % of total revenue across all vendors/payment types
    ROUND(
        e.total_revenue / NULLIF(tot.grand_total_revenue, 0) * 100.0,
        2
    )                                               AS total_revenue_share_pct,

    -- Trip share as % of total trips
    ROUND(
        CAST(e.total_trips AS NUMERIC) / NULLIF(tot.grand_total_trips, 0) * 100.0,
        2
    )                                               AS total_trip_share_pct

FROM economics e
CROSS JOIN totals tot
LEFT JOIN avg_hourly_by_vendor ah
    ON e.vendor_name = ah.vendor_name

ORDER BY
    e.vendor_name,
    e.total_revenue DESC
