"""
DAG: nyc_taxi_ingestion
Description: Ingests NYC Yellow Taxi monthly parquet data from the NYC TLC public
             dataset into Snowflake RAW_DATA schema. Runs daily but only downloads
             the prior month's data file when the month rolls over.

Pipeline steps:
    1. check_api_health      - Verify NYC Open Data endpoint is reachable
    2. download_taxi_data    - Download prior month's parquet file to /tmp/
    3. upload_to_snowflake_stage - PUT file to Snowflake internal stage
    4. load_to_raw_table     - COPY INTO RAW_DATA.NYC_TAXI_TRIPS from stage
    5. validate_row_count    - Assert >= 100,000 rows loaded
    6. trigger_dbt_bronze    - Run dbt bronze models
    7. notify_success        - Log completion summary
"""

from __future__ import annotations

import logging
import os
from datetime import datetime, timedelta
from pathlib import Path

import requests

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator

log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# DAG-level constants
# ---------------------------------------------------------------------------
DAG_ID = "nyc_taxi_ingestion"
SNOWFLAKE_CONN_ID = "snowflake_default"
NYC_TLC_BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data"
NYC_OPEN_DATA_HEALTH_URL = "https://data.cityofnewyork.us/api/views/metadata/v1"
TMP_DIR = Path("/tmp/nyc_taxi")
RAW_TABLE = "NYC_TAXI_LAKEHOUSE.RAW_DATA.NYC_TAXI_TRIPS"
STAGE_NAME = "@~/taxi_stage"
DBT_PROFILES_DIR = "/opt/airflow/dbt_project"
DBT_PROJECT_DIR = "/opt/airflow/dbt_project"

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

def _check_api_health(**context) -> None:
    """
    Verify that the NYC Open Data API is reachable before attempting download.
    Raises requests.exceptions.RequestException on failure.
    """
    url = NYC_OPEN_DATA_HEALTH_URL
    log.info("Checking NYC Open Data API health: %s", url)

    try:
        resp = requests.get(url, timeout=30)
        resp.raise_for_status()
        log.info("NYC Open Data API is healthy. Status: %d", resp.status_code)
    except requests.exceptions.Timeout:
        raise RuntimeError(
            f"NYC Open Data API health check timed out after 30s: {url}"
        )
    except requests.exceptions.ConnectionError as exc:
        raise RuntimeError(
            f"Could not connect to NYC Open Data API at {url}: {exc}"
        )
    except requests.exceptions.HTTPError as exc:
        raise RuntimeError(
            f"NYC Open Data API returned an error status: {exc}"
        )


def _download_taxi_data(**context) -> str:
    """
    Download the previous month's yellow taxi parquet file from NYC TLC.
    Files are published at:
        https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_YYYY-MM.parquet

    Returns the local file path via XCom.
    """
    execution_date: datetime = context["logical_date"]

    # Calculate the previous month (data is published one month in arrears)
    first_of_current_month = execution_date.replace(day=1)
    last_month = first_of_current_month - timedelta(days=1)
    year = last_month.year
    month = last_month.month

    filename = f"yellow_tripdata_{year}-{month:02d}.parquet"
    url = f"{NYC_TLC_BASE_URL}/{filename}"
    TMP_DIR.mkdir(parents=True, exist_ok=True)
    local_path = TMP_DIR / filename

    log.info("Downloading taxi data for %04d-%02d from: %s", year, month, url)
    log.info("Saving to: %s", local_path)

    with requests.get(url, stream=True, timeout=300) as resp:
        resp.raise_for_status()
        total_bytes = 0
        with open(local_path, "wb") as fh:
            for chunk in resp.iter_content(chunk_size=8 * 1024 * 1024):
                fh.write(chunk)
                total_bytes += len(chunk)

    size_mb = total_bytes / (1024 * 1024)
    log.info("Download complete. File size: %.2f MB", size_mb)

    # Push file path to XCom for downstream tasks
    context["task_instance"].xcom_push(key="local_file_path", value=str(local_path))
    context["task_instance"].xcom_push(key="filename", value=filename)
    context["task_instance"].xcom_push(key="year", value=year)
    context["task_instance"].xcom_push(key="month", value=month)

    return str(local_path)


def _upload_to_snowflake_stage(**context) -> None:
    """
    Upload the downloaded parquet file to a Snowflake internal stage using
    the SnowflakeHook. Uses the PUT command to compress and stage the file.
    """
    ti = context["task_instance"]
    local_file_path = ti.xcom_pull(task_ids="download_taxi_data", key="local_file_path")
    filename = ti.xcom_pull(task_ids="download_taxi_data", key="filename")

    log.info("Uploading %s to Snowflake stage %s", local_file_path, STAGE_NAME)

    hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
    conn = hook.get_conn()
    cursor = conn.cursor()

    try:
        put_sql = (
            f"PUT 'file://{local_file_path}' {STAGE_NAME}/ "
            f"AUTO_COMPRESS=TRUE OVERWRITE=TRUE"
        )
        log.info("Executing: %s", put_sql)
        cursor.execute(put_sql)
        result = cursor.fetchall()
        log.info("PUT result: %s", result)

        # Validate the PUT succeeded
        for row in result:
            status = row[6] if len(row) > 6 else "UNKNOWN"
            if status not in ("UPLOADED", "SKIPPED"):
                raise RuntimeError(
                    f"PUT command failed for {filename}. Status: {status}. Row: {row}"
                )

        log.info("File successfully staged: %s", filename)
        ti.xcom_push(key="staged_filename", value=f"{filename}.gz")

    finally:
        cursor.close()
        conn.close()


def _validate_row_count(**context) -> None:
    """
    Query the count of rows loaded into RAW_DATA.NYC_TAXI_TRIPS and assert
    that at least 100,000 rows were loaded. Raises ValueError if not met.
    """
    hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)

    count_sql = f"SELECT COUNT(*) FROM {RAW_TABLE}"
    log.info("Validating row count: %s", count_sql)

    result = hook.get_first(count_sql)
    row_count = result[0] if result else 0

    log.info("Row count in %s: %d", RAW_TABLE, row_count)

    MIN_EXPECTED_ROWS = 100_000
    if row_count < MIN_EXPECTED_ROWS:
        raise ValueError(
            f"Row count validation FAILED. Expected >= {MIN_EXPECTED_ROWS:,} rows, "
            f"but found {row_count:,} rows in {RAW_TABLE}. "
            "Check the COPY INTO logs for errors."
        )

    log.info(
        "Row count validation PASSED. %d rows loaded into %s.",
        row_count, RAW_TABLE
    )
    context["task_instance"].xcom_push(key="validated_row_count", value=row_count)


def _notify_success(**context) -> None:
    """
    Log a success summary including the row count validated in the previous task.
    In a production system, this could send a Slack message or PagerDuty alert.
    """
    ti = context["task_instance"]
    row_count = ti.xcom_pull(task_ids="validate_row_count", key="validated_row_count")
    year = ti.xcom_pull(task_ids="download_taxi_data", key="year")
    month = ti.xcom_pull(task_ids="download_taxi_data", key="month")
    execution_date = context["logical_date"]

    log.info(
        "SUCCESS: nyc_taxi_ingestion pipeline completed.\n"
        "  Execution date : %s\n"
        "  Data period    : %04d-%02d\n"
        "  Rows loaded    : %s\n"
        "  Target table   : %s",
        execution_date.isoformat(),
        year,
        month,
        f"{row_count:,}" if row_count else "N/A",
        RAW_TABLE,
    )


# ---------------------------------------------------------------------------
# DAG definition
# ---------------------------------------------------------------------------
with DAG(
    dag_id=DAG_ID,
    description="Ingest NYC Yellow Taxi monthly parquet data into Snowflake RAW layer",
    schedule_interval="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["ingestion", "nyc_taxi", "bronze"],
    doc_md=__doc__,
    max_active_runs=1,
) as dag:

    check_api_health = PythonOperator(
        task_id="check_api_health",
        python_callable=_check_api_health,
        doc_md="Check that the NYC Open Data API endpoint is reachable before downloading data.",
    )

    download_taxi_data = PythonOperator(
        task_id="download_taxi_data",
        python_callable=_download_taxi_data,
        doc_md="Download the previous month's yellow taxi parquet file from NYC TLC CloudFront.",
    )

    upload_to_snowflake_stage = PythonOperator(
        task_id="upload_to_snowflake_stage",
        python_callable=_upload_to_snowflake_stage,
        doc_md="PUT the downloaded parquet file to a Snowflake internal stage.",
    )

    load_to_raw_table = SnowflakeOperator(
        task_id="load_to_raw_table",
        snowflake_conn_id=SNOWFLAKE_CONN_ID,
        sql=f"""
            COPY INTO {RAW_TABLE}
            FROM {STAGE_NAME}/
            FILE_FORMAT = (
                TYPE = 'PARQUET'
                SNAPPY_COMPRESSION = TRUE
            )
            MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
            ON_ERROR = 'CONTINUE'
            PURGE = FALSE;
        """,
        doc_md="COPY INTO the raw table from the Snowflake internal stage using PARQUET format.",
    )

    validate_row_count = PythonOperator(
        task_id="validate_row_count",
        python_callable=_validate_row_count,
        doc_md="Assert that at least 100,000 rows were loaded into the raw table.",
    )

    trigger_dbt_bronze = BashOperator(
        task_id="trigger_dbt_bronze",
        bash_command=(
            f"dbt run "
            f"--select bronze "
            f"--profiles-dir {DBT_PROFILES_DIR} "
            f"--project-dir {DBT_PROJECT_DIR} "
            f"--no-partial-parse"
        ),
        doc_md="Run dbt bronze layer models to materialize cleaned raw data.",
    )

    notify_success = PythonOperator(
        task_id="notify_success",
        python_callable=_notify_success,
        doc_md="Log a success summary with row count and data period.",
    )

    # Task dependency chain
    (
        check_api_health
        >> download_taxi_data
        >> upload_to_snowflake_stage
        >> load_to_raw_table
        >> validate_row_count
        >> trigger_dbt_bronze
        >> notify_success
    )
