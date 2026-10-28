import logging

log = logging.getLogger("ingest")

INSERT_QUERY = """
    INSERT INTO transactions
        (transaction_id, customer_id, amount, event_timestamp)
    VALUES (%s, %s, %s, %s)
"""


def ingest_transactions(
    number_of_records: int = 100,
    newest_age_minutes: float = 0,
    seed: int | None = None,
) -> int:
    """Generate new transactions and insert them into PostgreSQL.

    Returns the number of rows inserted.
    """
    if number_of_records <= 0:
        raise ValueError("number_of_records must be > 0")

    if not DB_CONFIG["password"]:
        raise ValueError(f"POSTGRES_PASSWORD not found in {PROJECT_ROOT / '.env'}")

    with psycopg.connect(**DB_CONFIG) as conn, conn.cursor() as cur:
        # Serialize concurrent ingests so they can't pick the same start_id.
        cur.execute("LOCK TABLE transactions IN SHARE ROW EXCLUSIVE MODE")
        cur.execute("SELECT COALESCE(MAX(transaction_id), 0) + 1 FROM transactions")
        start_id = cur.fetchone()[0]

        transactions = generate_transactions(
            number_of_records,
            start_id=start_id,
            newest_age_minutes=newest_age_minutes,
            seed=seed,
        )

        rows = [
            (t["transaction_id"], t["customer_id"], t["amount"], t["event_timestamp"])
            for t in transactions
        ]

        cur.executemany(INSERT_QUERY, rows)
        inserted = cur.rowcount

    log.info("Generated %d transactions, inserted %d.", len(transactions), inserted)
    return inserted