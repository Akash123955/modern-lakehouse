"""
DAG: nyc_taxi_data_quality
Description: Weekly data quality monitoring pipeline for the NYC Taxi lakehouse.
             Checks data freshness, completeness, and aggregation integrity across
             all three lakehouse layers. Generates a JSON quality report and logs
             PASS/FAIL status for each check.

Pipeline steps:
    1. check_bronze_freshness     - Assert bronze data loaded within last 48 hours
    2. check_silver_completeness  - Check null rates on key silver columns
    3. check_gold_aggregations    - Verify gold revenue and trip counts are reasonable
    4. generate_quality_report    - Write JSON report to /tmp/
    5. alert_on_failures          - Log PASS/FAIL summary for all checks
"""

from __future__ import annotations

import json
import logging
from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
DAG_ID = "nyc_taxi_data_quality"
SNOWFLAKE_CONN_ID = "snowflake_default"
BRONZE_TRIPS_TABLE = "NYC_TAXI_LAKEHOUSE.BRONZE.BRONZE_TAXI_TRIPS"
SILVER_TRIPS_TABLE = "NYC_TAXI_LAKEHOUSE.SILVER.SILVER_TAXI_TRIPS"
GOLD_SUMMARY_TABLE = "NYC_TAXI_LAKEHOUSE.GOLD.GOLD_DAILY_TRIP_SUMMARY"
REPORT_DIR = Path("/tmp")

# Thresholds
BRONZE_FRESHNESS_HOURS = 48
SILVER_NULL_THRESHOLD_PCT = 5.0   # Alert if > 5% nulls on key columns
GOLD_MIN_DAILY_TRIPS = 1          # At least 1 trip per date/borough record
GOLD_MIN_DAILY_REVENUE = 0.01     # Revenue must be positive

# ---------------------------------------------------------------------------
# Default args
# ---------------------------------------------------------------------------
default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


# ---------------------------------------------------------------------------
# Task callables
# ---------------------------------------------------------------------------

def _check_bronze_freshness(**context) -> dict:
    """
    Query the MAX(_loaded_at) timestamp from the bronze trips table and assert
    it is within the last 48 hours. A stale bronze layer indicates the ingestion
    pipeline has failed or not run.
    """
    hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)

    sql = f"SELECT MAX(_loaded_at) FROM {BRONZE_TRIPS_TABLE}"
    log.info("Checking bronze freshness: %s", sql)

    result = hook.get_first(sql)
    max_loaded_at = result[0] if result else None

    check_result = {
        "check_name": "bronze_freshness",
        "max_loaded_at": str(max_loaded_at) if max_loaded_at else None,
        "threshold_hours": BRONZE_FRESHNESS_HOURS,
    }

    if max_loaded_at is None:
        check_result["status"] = "FAIL"
        check_result["message"] = "Bronze table has no data (_loaded_at is NULL)"
        log.error("Bronze freshness check FAILED: no data found")
    else:
        now = datetime.utcnow()
        # Ensure max_loaded_at is offset-naive for comparison
        if hasattr(max_loaded_at, "tzinfo") and max_loaded_at.tzinfo is not None:
            from datetime import timezone
            now = datetime.now(timezone.utc)

        age_hours = (now - max_loaded_at).total_seconds() / 3600.0
        check_result["age_hours"] = round(age_hours, 2)

        if age_hours > BRONZE_FRESHNESS_HOURS:
            check_result["status"] = "FAIL"
            check_result["message"] = (
                f"Bronze data is {age_hours:.1f} hours old, "
                f"exceeding threshold of {BRONZE_FRESHNESS_HOURS} hours."
            )
            log.error("Bronze freshness check FAILED: %s", check_result["message"])
        else:
            check_result["status"] = "PASS"
            check_result["message"] = (
                f"Bronze data is {age_hours:.1f} hours old (within {BRONZE_FRESHNESS_HOURS}h threshold)."
            )
            log.info("Bronze freshness check PASSED: %s", check_result["message"])

    context["task_instance"].xcom_push(key="bronze_freshness", value=check_result)
    return check_result


def _check_silver_completeness(**context) -> dict:
    """
    Query null counts for key columns in the silver trips table.
    Log results and flag any column exceeding the null threshold.
    """
    hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)

    key_columns = [
        "trip_id",
        "pickup_datetime",
        "dropoff_datetime",
        "pickup_location_id",
        "dropoff_location_id",
        "fare_amount",
        "total_amount",
        "trip_duration_minutes",
        "vendor_name",
        "payment_type_desc",
    ]

    # Build a single query to count nulls for all key columns
    null_count_exprs = ",\n        ".join(
        [f"SUM(CASE WHEN {col} IS NULL THEN 1 ELSE 0 END) AS null_{col}"
         for col in key_columns]
    )
    sql = f"""
        SELECT
            COUNT(*) AS total_rows,
            {null_count_exprs}
        FROM {SILVER_TRIPS_TABLE}
    """

    log.info("Checking silver completeness...")
    result = hook.get_first(sql)

    total_rows = result[0] if result else 0
    null_counts = {col: result[i + 1] for i, col in enumerate(key_columns)}

    column_checks = []
    any_failed = False

    for col, null_count in null_counts.items():
        null_pct = (null_count / total_rows * 100.0) if total_rows > 0 else 0.0
        status = "PASS" if null_pct <= SILVER_NULL_THRESHOLD_PCT else "FAIL"
        if status == "FAIL":
            any_failed = True
        column_checks.append({
            "column": col,
            "null_count": null_count,
            "null_pct": round(null_pct, 3),
            "threshold_pct": SILVER_NULL_THRESHOLD_PCT,
            "status": status,
        })
        log.info(
            "  %s: %d nulls (%.3f%%) - %s",
            col, null_count, null_pct, status
        )

    check_result = {
        "check_name": "silver_completeness",
        "total_rows": total_rows,
        "status": "FAIL" if any_failed else "PASS",
        "column_checks": column_checks,
        "message": (
            "Some key columns exceed null threshold"
            if any_failed
            else "All key columns within null threshold"
        ),
    }

    log.info(
        "Silver completeness check %s: %d total rows, %d columns checked",
        check_result["status"], total_rows, len(key_columns)
    )

    context["task_instance"].xcom_push(key="silver_completeness", value=check_result)
    return check_result


def _check_gold_aggregations(**context) -> dict:
    """
    Verify that gold daily trip summary aggregations are internally consistent:
    - total_revenue must always be positive
    - total_trips must always be >= 1
    Counts any violating rows.
    """
    hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)

    sql = f"""
        SELECT
            COUNT(*)                                          AS total_records,
            SUM(CASE WHEN total_revenue <= 0 THEN 1 ELSE 0 END) AS non_positive_revenue_count,
            SUM(CASE WHEN total_trips < 1 THEN 1 ELSE 0 END)    AS zero_trip_count,
            MIN(total_revenue)                                AS min_revenue,
            MAX(total_revenue)                                AS max_revenue,
            MIN(total_trips)                                  AS min_trips,
            MAX(total_trips)                                  AS max_trips
        FROM {GOLD_SUMMARY_TABLE}
    """

    log.info("Checking gold aggregations...")
    result = hook.get_first(sql)

    (
        total_records,
        non_positive_revenue_count,
        zero_trip_count,
        min_revenue,
        max_revenue,
        min_trips,
        max_trips,
    ) = result if result else (0, 0, 0, None, None, None, None)

    failed_checks = []
    if non_positive_revenue_count and non_positive_revenue_count > 0:
        failed_checks.append(
            f"{non_positive_revenue_count} records have non-positive revenue"
        )
    if zero_trip_count and zero_trip_count > 0:
        failed_checks.append(
            f"{zero_trip_count} records have zero trips"
        )

    check_result = {
        "check_name": "gold_aggregations",
        "total_records": total_records,
        "non_positive_revenue_count": non_positive_revenue_count or 0,
        "zero_trip_count": zero_trip_count or 0,
        "min_revenue": float(min_revenue) if min_revenue is not None else None,
        "max_revenue": float(max_revenue) if max_revenue is not None else None,
        "min_trips": int(min_trips) if min_trips is not None else None,
        "max_trips": int(max_trips) if max_trips is not None else None,
        "status": "FAIL" if failed_checks else "PASS",
        "message": "; ".join(failed_checks) if failed_checks else "All gold aggregations are valid",
    }

    log.info(
        "Gold aggregations check %s: %d records, revenue range [%.2f, %.2f]",
        check_result["status"],
        total_records,
        check_result["min_revenue"] or 0.0,
        check_result["max_revenue"] or 0.0,
    )

    context["task_instance"].xcom_push(key="gold_aggregations", value=check_result)
    return check_result


def _generate_quality_report(**context) -> str:
    """
    Collect all check results from XCom and write a consolidated JSON quality report
    to /tmp/quality_report_{YYYY-MM-DD}.json.
    """
    ti = context["task_instance"]
    execution_date = context["logical_date"]

    bronze_freshness = ti.xcom_pull(
        task_ids="check_bronze_freshness", key="bronze_freshness"
    ) or {}
    silver_completeness = ti.xcom_pull(
        task_ids="check_silver_completeness", key="silver_completeness"
    ) or {}
    gold_aggregations = ti.xcom_pull(
        task_ids="check_gold_aggregations", key="gold_aggregations"
    ) or {}

    all_checks = [bronze_freshness, silver_completeness, gold_aggregations]
    overall_status = "PASS" if all(c.get("status") == "PASS" for c in all_checks) else "FAIL"

    report = {
        "report_metadata": {
            "dag_id": DAG_ID,
            "execution_date": execution_date.isoformat(),
            "generated_at": datetime.utcnow().isoformat(),
            "overall_status": overall_status,
        },
        "checks": {
            "bronze_freshness": bronze_freshness,
            "silver_completeness": silver_completeness,
            "gold_aggregations": gold_aggregations,
        },
        "summary": {
            "total_checks": len(all_checks),
            "passed": sum(1 for c in all_checks if c.get("status") == "PASS"),
            "failed": sum(1 for c in all_checks if c.get("status") == "FAIL"),
        },
    }

    date_str = execution_date.strftime("%Y-%m-%d")
    report_path = REPORT_DIR / f"quality_report_{date_str}.json"

    with open(report_path, "w") as fh:
        json.dump(report, fh, indent=2, default=str)

    log.info("Quality report written to: %s", report_path)
    ti.xcom_push(key="report_path", value=str(report_path))
    ti.xcom_push(key="overall_status", value=overall_status)

    return str(report_path)


def _alert_on_failures(**context) -> None:
    """
    Read the quality report and log PASS/FAIL for each check.
    In production, failing checks would trigger PagerDuty/Slack alerts.
    """
    ti = context["task_instance"]
    report_path = ti.xcom_pull(task_ids="generate_quality_report", key="report_path")
    overall_status = ti.xcom_pull(task_ids="generate_quality_report", key="overall_status")

    if not report_path:
        log.error("No report path found - quality report may not have been generated.")
        return

    with open(report_path) as fh:
        report = json.load(fh)

    checks = report.get("checks", {})
    summary = report.get("summary", {})

    log.info("=" * 60)
    log.info("DATA QUALITY REPORT SUMMARY")
    log.info("=" * 60)
    log.info("Execution date : %s", report["report_metadata"]["execution_date"])
    log.info("Overall status : %s", overall_status)
    log.info("Checks passed  : %d / %d", summary.get("passed", 0), summary.get("total_checks", 0))
    log.info("-" * 60)

    for check_name, check_data in checks.items():
        status = check_data.get("status", "UNKNOWN")
        message = check_data.get("message", "No message")
        icon = "PASS" if status == "PASS" else "FAIL"
        log.info("[%s] %s: %s", icon, check_name, message)

    log.info("=" * 60)

    if overall_status == "FAIL":
        failed_checks = [
            name for name, data in checks.items()
            if data.get("status") == "FAIL"
        ]
        log.warning(
            "DATA QUALITY FAILURES DETECTED in checks: %s. "
            "Review the full report at: %s",
            ", ".join(failed_checks),
            report_path,
        )
    else:
        log.info("All data quality checks passed successfully.")


# ---------------------------------------------------------------------------
# DAG definition
# ---------------------------------------------------------------------------
with DAG(
    dag_id=DAG_ID,
    description="Weekly data quality monitoring for the NYC Taxi lakehouse",
    schedule_interval="@weekly",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["monitoring", "data_quality"],
    doc_md=__doc__,
    max_active_runs=1,
) as dag:

    check_bronze_freshness = PythonOperator(
        task_id="check_bronze_freshness",
        python_callable=_check_bronze_freshness,
        doc_md="Assert that bronze data was loaded within the last 48 hours.",
    )

    check_silver_completeness = PythonOperator(
        task_id="check_silver_completeness",
        python_callable=_check_silver_completeness,
        doc_md="Check null rates on key silver trip columns and log results.",
    )

    check_gold_aggregations = PythonOperator(
        task_id="check_gold_aggregations",
        python_callable=_check_gold_aggregations,
        doc_md="Verify gold daily totals have positive revenue and non-zero trip counts.",
    )

    generate_quality_report = PythonOperator(
        task_id="generate_quality_report",
        python_callable=_generate_quality_report,
        doc_md="Write consolidated JSON quality report to /tmp/quality_report_{date}.json.",
    )

    alert_on_failures = PythonOperator(
        task_id="alert_on_failures",
        python_callable=_alert_on_failures,
        doc_md="Read the quality report and log PASS/FAIL status for all checks.",
    )

    # Checks run in parallel; report and alert run sequentially after
    [
        check_bronze_freshness,
        check_silver_completeness,
        check_gold_aggregations,
    ] >> generate_quality_report >> alert_on_failures
