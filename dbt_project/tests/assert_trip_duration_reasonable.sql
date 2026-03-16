/*
    Test: assert_trip_duration_reasonable
    Model: silver_taxi_trips
    Type: Singular (custom) test

    Assertion: No trip in the silver layer should have a
               trip_duration_minutes value exceeding 300 minutes (5 hours).

    A non-empty result set means the test FAILS.
    An empty result set means the test PASSES.

    Rationale: While extremely long taxi trips are theoretically possible,
               trips exceeding 5 hours are almost certainly data entry errors
               or meter-left-running incidents. The NYC TLC defines reasonable
               fare disputes based on similar thresholds. Trips this long
               would skew aggregation metrics in the gold layer.

    Threshold: 300 minutes (configurable via dbt var 'nyc_taxi_max_trip_duration_minutes')
*/

SELECT
    trip_id,
    pickup_datetime,
    dropoff_datetime,
    trip_duration_minutes,
    fare_amount,
    total_amount,
    vendor_name,
    pickup_location_id,
    dropoff_location_id

FROM {{ ref('silver_taxi_trips') }}

WHERE trip_duration_minutes > {{ var('nyc_taxi_max_trip_duration_minutes', 300) }}
