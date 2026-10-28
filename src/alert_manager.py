import os
import logging
from pathlib import Path

import psycopg
from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).resolve().parent.parent
load_dotenv(PROJECT_ROOT / ".env")

DB_CONFIG = {
    "host": "localhost",
    "port": 5433,
    "dbname": "freshness_db",
    "user": "akshay",
    "password": os.getenv("POSTGRES_PASSWORD"),
}

FRESHNESS_THRESHOLD_MINUTES = 15

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)

logger = logging.getLogger(__name__)


def get_connection():
    """Connect to PostgreSQL."""
    if not DB_CONFIG["password"]:
        raise ValueError("POSTGRES_PASSWORD is missing from .env")
    return psycopg.connect(**DB_CONFIG)


def collect_monitoring_metrics():
    """
    Calculate freshness and ingestion latency,
    save the results in pipeline_metrics, and return them.
    """

    query = """
        SELECT
            MAX(event_timestamp),
            COUNT(*),
            ROUND(
                AVG(EXTRACT(EPOCH FROM (ingested_at - event_timestamp)))::NUMERIC,
                2
            )
        FROM transactions;
    """

    with get_connection() as connection:
        with connection.cursor() as cursor:
            cursor.execute(query)
            latest_timestamp, total_records, average_latency = cursor.fetchone()

            if total_records == 0 or latest_timestamp is None:
                freshness_minutes = None
                average_latency = None
                status = "NO_DATA"

                logger.error(
                    "NO_DATA | Dataset=transactions | No transaction records found."
                )

            else:
                cursor.execute(
                    """
                    SELECT EXTRACT(
                        EPOCH FROM (CURRENT_TIMESTAMP - %s)
                    ) / 60.0;
                    """,
                    (latest_timestamp,),
                )

                freshness_minutes = float(cursor.fetchone()[0])

                if freshness_minutes < 0:
                    status = "ERROR"
                    logger.error(
                        "INVALID_TIMESTAMP | Latest event timestamp is in the future."
                    )

                elif freshness_minutes > FRESHNESS_THRESHOLD_MINUTES:
                    status = "STALE"
                    logger.warning(
                        "STALE | Dataset=transactions | "
                        "Freshness=%.2f minutes | Threshold=%.2f minutes",
                        freshness_minutes,
                        FRESHNESS_THRESHOLD_MINUTES,
                    )

                else:
                    status = "HEALTHY"
                    logger.info(
                        "HEALTHY | Dataset=transactions | Freshness=%.2f minutes",
                        freshness_minutes,
                    )

            cursor.execute(
                """
                INSERT INTO pipeline_metrics (
                    dataset_name,
                    latest_data_timestamp,
                    checked_at,
                    freshness_minutes,
                    latency_seconds,
                    status
                )
                VALUES (
                    'transactions',
                    %s,
                    CURRENT_TIMESTAMP,
                    %s,
                    %s,
                    %s
                )
                RETURNING id;
                """,
                (
                    latest_timestamp,
                    freshness_minutes,
                    average_latency,
                    status,
                ),
            )

            metric_id = cursor.fetchone()[0]

    logger.info(
        "METRICS_SAVED | Metric ID=%s | Dataset=transactions | Status=%s",
        metric_id,
        status,
    )

    return {
        "metric_id": metric_id,
        "dataset": "transactions",
        "status": status,
        "freshness_minutes": (
            round(freshness_minutes, 2)
            if freshness_minutes is not None
            else None
        ),
        "average_latency_seconds": average_latency,
        "alert": status != "HEALTHY",
    }


def check_pipeline_alerts():
    """Find pipeline failures and SLA deadline violations."""

    query = """
        SELECT
            run_id,
            pipeline_name,
            status,
            completed_at,
            sla_deadline
        FROM pipeline_runs
        WHERE
            status IN ('FAILED', 'SLA_BREACHED')
            OR (
                completed_at IS NOT NULL
                AND sla_deadline IS NOT NULL
                AND completed_at > sla_deadline
            )
            OR (
                completed_at IS NULL
                AND sla_deadline IS NOT NULL
                AND CURRENT_TIMESTAMP > sla_deadline
                AND status = 'RUNNING'
            )
        ORDER BY run_id DESC;
    """

    with get_connection() as connection:
        with connection.cursor() as cursor:
            cursor.execute(query)
            rows = cursor.fetchall()

    if not rows:
        logger.info("PIPELINE_HEALTHY | No failed pipelines or SLA breaches found.")
        return []

    alerts = []

    for run_id, name, status, completed_at, deadline in rows:
        if status == "FAILED":
            alert_type = "PIPELINE_FAILED"
        elif status == "SLA_BREACHED":
            alert_type = "SLA_BREACHED"
        elif completed_at is None:
            alert_type = "SLA_BREACHED_RUNNING"
        else:
            alert_type = "SLA_BREACHED"

        logger.error(
            "%s | Run ID=%s | Pipeline=%s | Status=%s | "
            "Completed=%s | Deadline=%s",
            alert_type,
            run_id,
            name,
            status,
            completed_at,
            deadline,
        )

        alerts.append(
            {
                "run_id": run_id,
                "pipeline_name": name,
                "alert_type": alert_type,
                "status": status,
            }
        )

    return alerts


def run_alert_manager():
    """Execute monitoring checks and collect metrics."""

    logger.info("Starting Data Freshness and SLA Monitoring")

    freshness_result = collect_monitoring_metrics()
    pipeline_alerts = check_pipeline_alerts()

    logger.info("Monitoring checks completed.")

    print("\n" + "=" * 50)
    print("MONITORING SUMMARY")
    print("=" * 50)
    print(f"Dataset: {freshness_result['dataset']}")
    print(f"Status: {freshness_result['status']}")
    print(f"Freshness: {freshness_result['freshness_minutes']} minutes")
    print(
        "Average ingestion latency: "
        f"{freshness_result['average_latency_seconds']} seconds"
    )
    print(f"Metric ID: {freshness_result['metric_id']}")
    print(f"Pipeline alerts: {len(pipeline_alerts)}")
    print("=" * 50)

    return {
        "freshness": freshness_result,
        "pipeline_alerts": pipeline_alerts,
    }


if __name__ == "__main__":
    try:
        run_alert_manager()
    except Exception:
        logger.exception("Alert manager execution failed.")
        raise