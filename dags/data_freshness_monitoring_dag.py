from datetime import timedelta
import pendulum
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.trigger_rule import TriggerRule

PROJECT_DIR = "/opt/airflow/project"

default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
    "execution_timeout": timedelta(minutes=10),
}


def run_module(task_id: str, module: str, **kwargs) -> BashOperator:
    """Run `python -m <module>` from the project directory."""
    return BashOperator(
        task_id=task_id,
        bash_command=f"cd {PROJECT_DIR} && python -m {module}",
        **kwargs,
    )


with DAG(
    dag_id="data_freshness_sla_monitoring",
    description="Ingest transactions, check freshness and latency, and monitor SLAs",
    default_args=default_args,
    start_date=pendulum.datetime(2026, 10, 1, tz="UTC"),
    schedule="*/15 * * * *",
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=timedelta(minutes=14),
    tags=["data-engineering", "monitoring", "postgres"],
) as dag:

    # Ingestion generates its own records, so no separate generate task.
    ingest_data = run_module("ingest_data", "src.data_ingestion")

    # Monitors run even if ingestion failed, and don't retry (a retry
    # would only delay the alert).
    monitor_kwargs = {"trigger_rule": TriggerRule.ALL_DONE, "retries": 0}

    check_freshness = run_module(
        "check_data_freshness", "src.freshness_checker", **monitor_kwargs
    )
    calculate_latency = run_module(
        "calculate_ingestion_latency", "src.latency_calculator", **monitor_kwargs
    )
    run_alert_manager = run_module(
        "run_alert_manager", "src.alert_manager", **monitor_kwargs
    )

    ingest_data >> [check_freshness, calculate_latency] >> run_alert_manager