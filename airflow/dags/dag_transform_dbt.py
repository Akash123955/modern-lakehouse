"""
DAG: nyc_taxi_dbt_transform
Description: Runs the full dbt transformation pipeline (silver + gold layers)
             after the ingestion DAG completes. Uses ExternalTaskSensor to wait
             for the nyc_taxi_ingestion DAG to finish before proceeding.

Pipeline steps:
    1. wait_for_ingestion    - ExternalTaskSensor watching ingestion DAG
    2. dbt_test_bronze       - Run data quality tests on bronze layer
    3. dbt_run_silver        - Build silver (cleaned) models
    4. dbt_test_silver       - Run data quality tests on silver layer
    5. dbt_run_gold          - Build gold (analytics) models
    6. dbt_test_gold         - Run data quality tests on gold layer
    7. dbt_generate_docs     - Generate dbt documentation site
    8. notify_transform_complete - Log completion with timestamp
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.sensors.external_task import ExternalTaskSensor

log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
DAG_ID = "nyc_taxi_dbt_transform"
UPSTREAM_DAG_ID = "nyc_taxi_ingestion"
UPSTREAM_TASK_ID = "notify_success"
DBT_PROFILES_DIR = "/opt/airflow/dbt_project"
DBT_PROJECT_DIR = "/opt/airflow/dbt_project"

DBT_BASE_CMD = (
    f"dbt {{subcommand}} "
    f"--profiles-dir {DBT_PROFILES_DIR} "
    f"--project-dir {DBT_PROJECT_DIR} "
    f"--no-partial-parse"
)

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

def _notify_transform_complete(**context) -> None:
    """
    Log completion summary for the transformation pipeline.
    In production, this would send a Slack/Teams notification or update
    a data catalog with the latest run metadata.
    """
    execution_date = context["logical_date"]
    end_time = datetime.utcnow()

    log.info(
        "SUCCESS: nyc_taxi_dbt_transform pipeline completed.\n"
        "  Execution date : %s\n"
        "  Completed at   : %s (UTC)\n"
        "  Layers built   : bronze (validated), silver, gold\n"
        "  Docs generated : /opt/airflow/dbt_project/target/index.html",
        execution_date.isoformat(),
        end_time.isoformat(),
    )


# ---------------------------------------------------------------------------
# DAG definition
# ---------------------------------------------------------------------------
with DAG(
    dag_id=DAG_ID,
    description="Run dbt silver and gold transformations after ingestion completes",
    schedule_interval="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["transformation", "dbt", "silver", "gold"],
    doc_md=__doc__,
    max_active_runs=1,
) as dag:

    wait_for_ingestion = ExternalTaskSensor(
        task_id="wait_for_ingestion",
        external_dag_id=UPSTREAM_DAG_ID,
        external_task_id=UPSTREAM_TASK_ID,
        timeout=3600,
        allowed_states=["success"],
        failed_states=["failed", "skipped"],
        mode="reschedule",
        poke_interval=60,
        doc_md=(
            "Wait for the nyc_taxi_ingestion DAG's notify_success task to complete "
            "before starting the dbt transformation pipeline."
        ),
    )

    dbt_test_bronze = BashOperator(
        task_id="dbt_test_bronze",
        bash_command=DBT_BASE_CMD.format(subcommand="test --select bronze"),
        doc_md="Run dbt data quality tests against the bronze layer models.",
    )

    dbt_run_silver = BashOperator(
        task_id="dbt_run_silver",
        bash_command=DBT_BASE_CMD.format(subcommand="run --select silver"),
        doc_md="Build silver layer models (cleaned, typed, business rules applied).",
    )

    dbt_test_silver = BashOperator(
        task_id="dbt_test_silver",
        bash_command=DBT_BASE_CMD.format(subcommand="test --select silver"),
        doc_md="Run dbt data quality tests against the silver layer models.",
    )

    dbt_run_gold = BashOperator(
        task_id="dbt_run_gold",
        bash_command=DBT_BASE_CMD.format(subcommand="run --select gold"),
        doc_md="Build gold layer models (analytics-ready aggregates).",
    )

    dbt_test_gold = BashOperator(
        task_id="dbt_test_gold",
        bash_command=DBT_BASE_CMD.format(subcommand="test --select gold"),
        doc_md="Run dbt data quality tests against the gold layer models.",
    )

    dbt_generate_docs = BashOperator(
        task_id="dbt_generate_docs",
        bash_command=DBT_BASE_CMD.format(subcommand="docs generate"),
        doc_md="Generate the dbt documentation catalog and manifest.",
    )

    notify_transform_complete = PythonOperator(
        task_id="notify_transform_complete",
        python_callable=_notify_transform_complete,
        doc_md="Log transformation completion summary with timestamp.",
    )

    # Task dependency chain
    (
        wait_for_ingestion
        >> dbt_test_bronze
        >> dbt_run_silver
        >> dbt_test_silver
        >> dbt_run_gold
        >> dbt_test_gold
        >> dbt_generate_docs
        >> notify_transform_complete
    )
