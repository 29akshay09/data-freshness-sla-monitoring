import logging
import os
import sys
from enum import Enum
from pathlib import Path

import psycopg
from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).resolve().parent.parent
load_dotenv(PROJECT_ROOT / ".env")

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("freshness")

FRESHNESS_THRESHOLD_MINUTES = float(os.getenv("FRESHNESS_THRESHOLD_MINUTES", 15))
CLOCK_SKEW_TOLERANCE_MINUTES = 1.0


class Status(str, Enum):
    HEALTHY = "HEALTHY"
    STALE = "STALE"
    NO_DATA = "NO_DATA"
    INVALID_TIMESTAMP = "INVALID_TIMESTAMP"
    ERROR = "ERROR"


def get_db_config() -> dict:
    password = os.getenv("POSTGRES_PASSWORD")
    if not password:
        raise ValueError("POSTGRES_PASSWORD is missing from .env")
    return {
        "host": os.getenv("POSTGRES_HOST", "localhost"),
        "port": int(os.getenv("POSTGRES_PORT", 5433)),
        "dbname": os.getenv("POSTGRES_DB", "freshness_db"),
        "user": os.getenv("POSTGRES_USER", "akshay"),
        "password": password,
        "connect_timeout": 10,
        "options": "-c statement_timeout=30000",
    }


QUERY = """
    SELECT
        MAX(event_timestamp),
        EXTRACT(EPOCH FROM (NOW() - MAX(event_timestamp))) / 60
    FROM transactions;
"""


def check_freshness() -> dict:
    try:
        with psycopg.connect(**get_db_config()) as conn, conn.cursor() as cur:
            cur.execute(QUERY)
            latest, minutes = cur.fetchone()
    except (psycopg.Error, ValueError) as exc:
        log.error("Freshness check failed: %s", exc)
        return {"dataset_name": "transactions", "latest_timestamp": None,
                "freshness_minutes": None, "status": Status.ERROR}

    if latest is None:
        status, freshness = Status.NO_DATA, None
    else:
        freshness = float(minutes)
        if freshness < -CLOCK_SKEW_TOLERANCE_MINUTES:
            status = Status.INVALID_TIMESTAMP
        elif freshness > FRESHNESS_THRESHOLD_MINUTES:
            status = Status.STALE
        else:
            status = Status.HEALTHY

    return {"dataset_name": "transactions", "latest_timestamp": latest,
            "freshness_minutes": freshness, "status": status}


def report(result: dict) -> None:
    fm = result["freshness_minutes"]
    log.info(
        "dataset=%s latest=%s freshness=%s threshold=%s status=%s",
        result["dataset_name"],
        result["latest_timestamp"],
        f"{fm:.2f}m" if fm is not None else "n/a",
        f"{FRESHNESS_THRESHOLD_MINUTES}m",
        result["status"].value,
    )


if __name__ == "__main__":
    result = check_freshness()
    report(result)
    sys.exit(0 if result["status"] is Status.HEALTHY else 1)