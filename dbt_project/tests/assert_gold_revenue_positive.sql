/*
    Test: assert_gold_revenue_positive
    Model: gold_daily_trip_summary
    Type: Singular (custom) test

    Assertion: Every row in gold_daily_trip_summary must have a
               total_revenue value strictly greater than zero.

    A non-empty result set means the test FAILS.
    An empty result set means the test PASSES.

    Rationale: Revenue aggregates must always be positive since we
               filter out zero and negative fare trips in the silver layer.
               A zero or negative aggregate at the gold level would indicate
               a data quality issue in the aggregation logic or an upstream
               filter failure.
*/

SELECT
    trip_date,
    pickup_borough,
    total_trips,
    total_revenue

FROM {{ ref('gold_daily_trip_summary') }}

WHERE total_revenue <= 0
