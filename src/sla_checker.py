import logging
import os
import sys
from datetime import datetime, timezone
from enum import Enum
from pathlib import Path

import psycopg
from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).resolve().parent.parent
load_dotenv(PROJECT_ROOT / ".env")

log = logging.getLogger("sla")

DB_CONFIG = {
    "host": "localhost",
    "port": 5433,
    "dbname": "freshness_db",
    "user": "akshay",
    "password": os.getenv("POSTGRES_PASSWORD"),
    "connect_timeout": 10,
    "options": "-c statement_timeout=30000",
}

# Adjust to match the values your pipeline_runs.status column actually uses.
FAILED_STATUSES = {"FAILED", "ERROR"}

QUERY = """
    SELECT DISTINCT ON (pipeline_name)
        run_id, pipeline_name, started_at, completed_at, status, sla_deadline
    FROM pipeline_runs
    ORDER BY pipeline_name, started_at DESC;
"""


class SlaResult(str, Enum):
    MET = "MET"
    BREACHED = "BREACHED"
    RUNNING = "RUNNING"
    FAILED = "FAILED"
    UNKNOWN = "UNKNOWN"
    INVALID = "INVALID"


BAD_RESULTS = {SlaResult.BREACHED, SlaResult.FAILED, SlaResult.INVALID}


def evaluate_run(started, completed, status, deadline, now) -> tuple[SlaResult, float | None]:
    """Return (result, margin_minutes). Positive margin = time to spare, negative = late."""
    if status and status.upper() in FAILED_STATUSES:
        return SlaResult.FAILED, None
    if completed is not None and started is not None and completed < started:
        return SlaResult.INVALID, None
    if deadline is None:
        return SlaResult.UNKNOWN, None

    if completed is None:
        margin = (deadline - now).total_seconds() / 60
        return (SlaResult.BREACHED if margin < 0 else SlaResult.RUNNING), margin

    margin = (deadline - completed).total_seconds() / 60
    return (SlaResult.MET if margin >= 0 else SlaResult.BREACHED), margin


def check_sla() -> list[dict]:
    if not DB_CONFIG["password"]:
        raise ValueError(f"POSTGRES_PASSWORD not found in {PROJECT_ROOT / '.env'}")

    with psycopg.connect(**DB_CONFIG) as conn, conn.cursor() as cur:
        cur.execute(QUERY)
        rows = cur.fetchall()

    now = datetime.now(timezone.utc)
    results = []
    for run_id, name, started, completed, status, deadline in rows:
        result, margin = evaluate_run(started, completed, status, deadline, now)
        results.append({
            "run_id": run_id,
            "pipeline_name": name,
            "started_at": started,
            "completed_at": completed,
            "recorded_status": status,
            "sla_deadline": deadline,
            "result": result,
            "margin_minutes": margin,
        })
    return results


def report(results: list[dict]) -> None:
    print("=" * 65)
    print("PIPELINE SLA MONITORING REPORT")
    print("=" * 65)

    if not results:
        print("NO_DATA: no pipeline runs found.")
    for r in results:
        margin = r["margin_minutes"]
        margin_text = "n/a" if margin is None else f"{margin:+.1f} min"
        print(f"\nPipeline: {r['pipeline_name']} | Run ID: {r['run_id']}")
        print(f"Started: {r['started_at']}  Completed: {r['completed_at']}")
        print(f"Recorded status: {r['recorded_status']}  SLA deadline: {r['sla_deadline']}")
        print(f"SLA result: {r['result'].value} (margin {margin_text})")

    print("\n" + "=" * 65)


if __name__ == "__main__":
    results = check_sla()
    report(results)
    sys.exit(1 if any(r["result"] in BAD_RESULTS for r in results) else 0)