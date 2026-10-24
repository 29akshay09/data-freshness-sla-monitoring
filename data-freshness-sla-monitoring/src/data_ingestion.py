import os
from pathlib import Path

import psycopg
from dotenv import load_dotenv

from src.data_generator import generate_transactions

# Load .env from the project root.
PROJECT_ROOT = Path(__file__).resolve().parent.parent
load_dotenv(PROJECT_ROOT / ".env")

DB_CONFIG = {
    "host": "localhost",
    "port": 5433,
    "dbname": "freshness_db",
    "user": "akshay",
    "password": os.getenv("POSTGRES_PASSWORD"),
}

INSERT_QUERY = """
    INSERT INTO transactions (
        transaction_id,
        customer_id,
        amount,
        event_timestamp,
        ingested_at
    )
    VALUES (%s, %s, %s, %s, NOW())
    ON CONFLICT (transaction_id) DO NOTHING;
"""


def ingest_transactions(number_of_records=100):
    """Generate transactions and insert them into PostgreSQL."""
    if not DB_CONFIG["password"]:
        raise ValueError(f"POSTGRES_PASSWORD not found in {PROJECT_ROOT / '.env'}")

    transactions = generate_transactions(number_of_records)
    rows = [
        (r["transaction_id"], r["customer_id"], r["amount"], r["event_timestamp"])
        for r in transactions
    ]

    with psycopg.connect(**DB_CONFIG) as connection:
        with connection.cursor() as cursor:
            cursor.executemany(INSERT_QUERY, rows)
            inserted = cursor.rowcount

    print(f"Generated {len(transactions)} transactions, inserted {inserted}.")
    print(f"Skipped {len(transactions) - inserted} already existing IDs.")


if __name__ == "__main__":
    ingest_transactions()