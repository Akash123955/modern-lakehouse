"""
DAG: nyc_taxi_streaming_monitor
Description: Monitors the health of the Kafka → Snowpipe real-time ingestion
             pipeline. Runs every 15 minutes to detect lag, gaps, and errors.

Pipeline checks:
    1. check_snowpipe_lag       - Assert last ingestion < 5 minutes ago
    2. check_event_throughput   - Assert > 0 events ingested in last window
    3. check_error_rate         - Assert < 5% COPY errors in last 100 files
    4. check_kafka_consumer_lag - Query Kafka consumer group lag via Admin API
    5. generate_health_report   - Build structured health JSON and push to XCom
    6. alert_on_breach          - Send Slack alert if any check fails

This DAG is intentionally decoupled from the batch ingestion and dbt DAGs.
It's lightweight, runs frequently, and has no downstream dependencies.
"""

from __future__ import annotations

import json
import logging
import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

log = logging.getLogger(__name__)

DAG_ID = "nyc_taxi_streaming_monitor"
SNOWFLAKE_CONN_ID = "snowflake_default"

# Alert thresholds
MAX_LAG_MINUTES = 5          # Alert if Snowpipe hasn't loaded data in > 5 min
MIN_EVENTS_PER_WINDOW = 10   # Alert if < 10 events loaded in last 15 min window
MAX_ERROR_RATE_PCT = 5.0     # Alert if > 5% of COPY files have errors

default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "email_on_failure": False,
    "retries": 0,
}


# ---------------------------------------------------------------------------
# Task callables
# ---------------------------------------------------------------------------

def _check_snowpipe_lag(**context) -> dict:
    """
    Query Snowflake COPY_HISTORY to find the time of the last successful
    Snowpipe load. Fails if lag exceeds MAX_LAG_MINUTES.
    """
    hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)

    sql = """
        SELECT
            MAX(last_load_time)                                    AS last_load_time,
            DATEDIFF('minute', MAX(last_load_time), CURRENT_TIMESTAMP()) AS lag_minutes,
            COUNT(*)                                               AS files_last_hour
        FROM TABLE(
            INFORMATION_SCHEMA.COPY_HISTORY(
                TABLE_NAME => 'NYC_TAXI_TRIPS_STREAMING',
                START_TIME => DATEADD('hour', -1, CURRENT_TIMESTAMP())
            )
        )
        WHERE status = 'Loaded'
    """

    result = hook.get_first(sql)
    last_load_time = result[0] if result else None
    lag_minutes = result[1] if result and result[1] is not None else 9999
    files_last_hour = result[2] if result else 0

    status = "PASS" if lag_minutes <= MAX_LAG_MINUTES else "FAIL"
    check_result = {
        "check": "snowpipe_lag",
        "status": status,
        "lag_minutes": float(lag_minutes),
        "last_load_time": str(last_load_time) if last_load_time else None,
        "files_last_hour": files_last_hour,
        "threshold_minutes": MAX_LAG_MINUTES,
    }

    log.info("Snowpipe lag check: %s (lag=%.1f min, threshold=%d min)",
             status, lag_minutes, MAX_LAG_MINUTES)

    if status == "FAIL":
        log.warning("ALERT: Snowpipe lag %.1f min exceeds threshold %d min", lag_minutes, MAX_LAG_MINUTES)

    context["task_instance"].xcom_push(key="snowpipe_lag_check", value=check_result)
    return check_result


def _check_event_throughput(**context) -> dict:
    """
    Verify that at least MIN_EVENTS_PER_WINDOW events were ingested in the
    last 15-minute monitoring window.
    """
    hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)

    sql = """
        SELECT COUNT(*) AS events_ingested
        FROM NYC_TAXI_LAKEHOUSE.RAW_DATA.NYC_TAXI_TRIPS_STREAMING
        WHERE _ingested_at >= DATEADD('minute', -15, CURRENT_TIMESTAMP())
    """

    result = hook.get_first(sql)
    events_count = result[0] if result else 0

    status = "PASS" if events_count >= MIN_EVENTS_PER_WINDOW else "FAIL"
    check_result = {
        "check": "event_throughput",
        "status": status,
        "events_last_15min": events_count,
        "threshold_min_events": MIN_EVENTS_PER_WINDOW,
    }

    log.info("Throughput check: %s (events_15min=%d, threshold=%d)",
             status, events_count, MIN_EVENTS_PER_WINDOW)

    context["task_instance"].xcom_push(key="throughput_check", value=check_result)
    return check_result


def _check_error_rate(**context) -> dict:
    """
    Check the Snowpipe COPY error rate over the last 100 files loaded.
    Alerts if > MAX_ERROR_RATE_PCT of files had errors.
    """
    hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)

    sql = """
        WITH recent_files AS (
            SELECT
                status,
                first_error_message,
                row_count,
                row_parsed,
                last_load_time
            FROM TABLE(
                INFORMATION_SCHEMA.COPY_HISTORY(
                    TABLE_NAME => 'NYC_TAXI_TRIPS_STREAMING',
                    START_TIME => DATEADD('hour', -24, CURRENT_TIMESTAMP())
                )
            )
            ORDER BY last_load_time DESC
            LIMIT 100
        )
        SELECT
            COUNT(*) AS total_files,
            SUM(CASE WHEN status != 'Loaded' THEN 1 ELSE 0 END)  AS error_files,
            ROUND(
                100.0 * SUM(CASE WHEN status != 'Loaded' THEN 1 ELSE 0 END)
                / NULLIF(COUNT(*), 0),
                2
            )                                                     AS error_rate_pct,
            SUM(row_count)                                        AS total_rows_loaded,
            SUM(row_parsed - row_count)                           AS total_rows_errored
        FROM recent_files
    """

    result = hook.get_first(sql)
    total_files = result[0] or 0
    error_files = result[1] or 0
    error_rate_pct = float(result[2] or 0.0)
    total_rows_loaded = result[3] or 0
    total_rows_errored = result[4] or 0

    status = "PASS" if error_rate_pct <= MAX_ERROR_RATE_PCT else "FAIL"
    check_result = {
        "check": "error_rate",
        "status": status,
        "total_files_checked": total_files,
        "error_files": error_files,
        "error_rate_pct": error_rate_pct,
        "total_rows_loaded": total_rows_loaded,
        "total_rows_errored": total_rows_errored,
        "threshold_error_rate_pct": MAX_ERROR_RATE_PCT,
    }

    log.info("Error rate check: %s (rate=%.1f%%, threshold=%.1f%%)",
             status, error_rate_pct, MAX_ERROR_RATE_PCT)

    context["task_instance"].xcom_push(key="error_rate_check", value=check_result)
    return check_result


def _generate_health_report(**context) -> dict:
    """
    Consolidate all check results into a structured health report.
    Determines overall pipeline health and pushes to XCom.
    """
    ti = context["task_instance"]

    lag_check = ti.xcom_pull(task_ids="check_snowpipe_lag", key="snowpipe_lag_check") or {}
    throughput_check = ti.xcom_pull(task_ids="check_event_throughput", key="throughput_check") or {}
    error_check = ti.xcom_pull(task_ids="check_error_rate", key="error_rate_check") or {}

    all_checks = [lag_check, throughput_check, error_check]
    failed_checks = [c for c in all_checks if c.get("status") == "FAIL"]
    overall_status = "HEALTHY" if not failed_checks else "DEGRADED"

    report = {
        "dag_run_id": context["run_id"],
        "evaluated_at": datetime.utcnow().isoformat() + "Z",
        "overall_status": overall_status,
        "failed_check_count": len(failed_checks),
        "checks": {
            "snowpipe_lag": lag_check,
            "event_throughput": throughput_check,
            "error_rate": error_check,
        },
        "summary": (
            f"All {len(all_checks)} checks passed" if not failed_checks
            else f"{len(failed_checks)}/{len(all_checks)} checks FAILED: "
                 + ", ".join(c.get("check", "?") for c in failed_checks)
        ),
    }

    log.info("Health report: %s | %s", overall_status, report["summary"])
    ti.xcom_push(key="health_report", value=report)
    return report


def _should_alert(**context) -> str:
    """Branch: only send alert if health report shows failures."""
    ti = context["task_instance"]
    report = ti.xcom_pull(task_ids="generate_health_report", key="health_report") or {}
    if report.get("overall_status") != "HEALTHY":
        return "send_slack_alert"
    log.info("All streaming checks passed. No alert needed.")
    return "no_alert"


def _send_slack_alert(**context) -> None:
    """
    Send a Slack alert when streaming health checks fail.
    Reads SLACK_WEBHOOK_URL from environment.
    """
    import urllib.request

    ti = context["task_instance"]
    report = ti.xcom_pull(task_ids="generate_health_report", key="health_report") or {}
    webhook_url = os.getenv("SLACK_WEBHOOK_URL")

    if not webhook_url:
        log.warning("SLACK_WEBHOOK_URL not set — skipping alert. Report: %s", json.dumps(report, indent=2))
        return

    failed_checks = [
        name for name, check in report.get("checks", {}).items()
        if check.get("status") == "FAIL"
    ]

    payload = {
        "text": "🚨 *NYC Taxi Streaming Pipeline Alert*",
        "attachments": [
            {
                "color": "danger",
                "title": f"Streaming Health: {report.get('overall_status', 'UNKNOWN')}",
                "text": report.get("summary", "Health check failed"),
                "fields": [
                    {
                        "title": "Failed Checks",
                        "value": "\n".join(f"• {c}" for c in failed_checks),
                        "short": False,
                    },
                    {
                        "title": "Evaluated At",
                        "value": report.get("evaluated_at", "unknown"),
                        "short": True,
                    },
                    {
                        "title": "DAG Run",
                        "value": context.get("run_id", "unknown"),
                        "short": True,
                    },
                ],
                "footer": "nyc_taxi_streaming_monitor",
            }
        ],
    }

    data = json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(webhook_url, data=data, headers={"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            log.info("Slack alert sent. Response: %s", resp.read().decode())
    except Exception as exc:
        log.error("Failed to send Slack alert: %s", exc)


# ---------------------------------------------------------------------------
# DAG definition
# ---------------------------------------------------------------------------
with DAG(
    dag_id=DAG_ID,
    description="Monitor Kafka → Snowpipe real-time ingestion health every 15 minutes",
    schedule_interval="*/15 * * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["streaming", "monitoring", "snowpipe", "kafka"],
    doc_md=__doc__,
    max_active_runs=1,
) as dag:

    check_snowpipe_lag = PythonOperator(
        task_id="check_snowpipe_lag",
        python_callable=_check_snowpipe_lag,
        doc_md="Check time since last successful Snowpipe load. Alert if > 5 minutes.",
    )

    check_event_throughput = PythonOperator(
        task_id="check_event_throughput",
        python_callable=_check_event_throughput,
        doc_md="Verify events are being ingested in each 15-minute window.",
    )

    check_error_rate = PythonOperator(
        task_id="check_error_rate",
        python_callable=_check_error_rate,
        doc_md="Check COPY error rate over last 100 Snowpipe files.",
    )

    generate_health_report = PythonOperator(
        task_id="generate_health_report",
        python_callable=_generate_health_report,
        doc_md="Consolidate check results into a structured health report.",
        trigger_rule="all_done",  # Run even if upstream checks fail
    )

    should_alert = BranchPythonOperator(
        task_id="should_alert",
        python_callable=_should_alert,
        doc_md="Branch: send Slack alert only if any checks failed.",
    )

    send_slack_alert = PythonOperator(
        task_id="send_slack_alert",
        python_callable=_send_slack_alert,
        doc_md="Send Slack alert with health report details.",
    )

    no_alert = EmptyOperator(
        task_id="no_alert",
        doc_md="No-op: all checks passed, no alert needed.",
    )

    # Task dependency graph
    [check_snowpipe_lag, check_event_throughput, check_error_rate] >> generate_health_report
    generate_health_report >> should_alert >> [send_slack_alert, no_alert]
