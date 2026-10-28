import random
from datetime import datetime, timedelta, timezone
from decimal import Decimal


def generate_transactions(
    number_of_records: int = 100,
    start_id: int = 1,
    newest_age_minutes: float = 0,
    window_minutes: float = 60,
    seed: int | None = None,
) -> list[dict]:
    """Generate sample transaction records for pipeline testing.

    newest_age_minutes: age of the most recent event; set above the
        freshness threshold to simulate a stale pipeline.
    window_minutes: how far back older events are spread.
    """
    if number_of_records < 0:
        raise ValueError("number_of_records must be >= 0")

    rng = random.Random(seed)
    newest = datetime.now(timezone.utc) - timedelta(minutes=newest_age_minutes)

    transactions = [
        {
            "transaction_id": start_id + i,
            "customer_id": rng.randint(1000, 9999),
            "amount": Decimal(rng.randint(10_000, 500_000)) / 100,
            "event_timestamp": newest - timedelta(
                seconds=rng.uniform(0, window_minutes * 60)
            ),
        }
        for i in range(number_of_records)
    ]

    transactions.sort(key=lambda t: t["event_timestamp"])
    return transactions


if __name__ == "__main__":
    records = generate_transactions(10, newest_age_minutes=30, seed=42)

    for record in records:
        print(record)

    print(f"\nGenerated {len(records)} transactions.")