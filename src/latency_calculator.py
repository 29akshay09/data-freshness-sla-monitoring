import logging
import os
from pathlib import Path

import psycopg
from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).resolve().parent.parent
load_dotenv(PROJECT_ROOT / ".env")

log = logging.getLogger("latency")

DB_CONFIG = {
    "host": "localhost",
    "port": 5433,
    "dbname": "freshness_db",
    "user": "akshay",
    "password": os.getenv("POSTGRES_PASSWORD"),
    "connect_timeout": 10,
    "options": "-c statement_timeout=30000",
}

QUERY = """
    WITH latencies AS (
        SELECT EXTRACT(EPOCH FROM (ingested_at - event_timestamp)) AS seconds
        FROM transactions
        WHERE ingested_at IS NOT NULL
          AND ingested_at >= NOW() - %s * INTERVAL '1 minute'
    )
    SELECT
        COUNT(*),
        AVG(seconds),
        percentile_cont(0.5)  WITHIN GROUP (ORDER BY seconds),
        percentile_cont(0.95) WITHIN GROUP (ORDER BY seconds),
        MIN(seconds),
        MAX(seconds),
        COUNT(*) FILTER (WHERE seconds < 0)
    FROM latencies;
"""


def _to_float(value):
    return float(value) if value is not None else None


def calculate_latency(window_minutes: float = 60) -> dict:
    """Event-to-ingestion latency for rows ingested in the last window."""
    if not DB_CONFIG["password"]:
        raise ValueError(f"POSTGRES_PASSWORD not found in {PROJECT_ROOT / '.env'}")

    with psycopg.connect(**DB_CONFIG) as conn, conn.cursor() as cur:
        cur.execute(QUERY, (float(window_minutes),))
        total, avg, p50, p95, minimum, maximum, negative = cur.fetchone()

    return {
        "window_minutes": window_minutes,
        "total_transactions": total,
        "average_latency_seconds": _to_float(avg),
        "p50_latency_seconds": _to_float(p50),
        "p95_latency_seconds": _to_float(p95),
        "minimum_latency_seconds": _to_float(minimum),
        "maximum_latency_seconds": _to_float(maximum),
        "negative_latency_rows": negative,
    }


def report(result: dict) -> None:
    print("=" * 45)
    print("PIPELINE LATENCY REPORT")
    print("=" * 45)
    print(f"Window: last {result['window_minutes']} minutes")
    print(f"Transactions: {result['total_transactions']}")

    if result["total_transactions"] == 0:
        print("No transactions in window.")
    else:
        for label, key in [
            ("Average", "average_latency_seconds"),
            ("p50", "p50_latency_seconds"),
            ("p95", "p95_latency_seconds"),
            ("Min", "minimum_latency_seconds"),
            ("Max", "maximum_latency_seconds"),
        ]:
            print(f"{label}: {result[key]:.2f} seconds")
        if result["negative_latency_rows"]:
            print(f"WARNING: {result['negative_latency_rows']} rows with negative latency")
    print("=" * 45)


if __name__ == "__main__":
    report(calculate_latency())