/*
    Test: assert_silver_trips_no_future_dates
    Model: silver_taxi_trips
    Type: Singular (custom) test

    Assertion: No trip in the silver layer should have a pickup_datetime
               that is in the future (greater than the current timestamp).

    A non-empty result set means the test FAILS.
    An empty result set means the test PASSES.

    Rationale: Future-dated trips indicate either a data ingestion error,
               timestamp parsing bug, or data quality issue in the source system.
               All pickup events must have already occurred.
*/

SELECT
    trip_id,
    pickup_datetime,
    dropoff_datetime,
    vendor_name,
    pickup_location_id,
    CURRENT_TIMESTAMP()     AS current_ts,
    DATEDIFF(
        'minute',
        CURRENT_TIMESTAMP(),
        pickup_datetime
    )                       AS minutes_in_future

FROM {{ ref('silver_taxi_trips') }}

WHERE pickup_datetime > CURRENT_TIMESTAMP()
