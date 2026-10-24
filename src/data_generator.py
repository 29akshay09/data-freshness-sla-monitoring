import random
from datetime import datetime, timedelta, timezone

def generate_transactions(number_of_records=100):
"""Generate sample transaction records for pipeline testing."""

```
now = datetime.now(timezone.utc)
transactions = []

for transaction_id in range(1, number_of_records + 1):
    event_timestamp = now - timedelta(
        minutes=random.randint(0, 60)
    )

    transaction = {
        "transaction_id": transaction_id,
        "customer_id": random.randint(1000, 9999),
        "amount": round(random.uniform(100, 5000), 2),
        "event_timestamp": event_timestamp,
    }

    transactions.append(transaction)

return transactions
```

if **name** == "**main**":
records = generate_transactions(10)

```
for record in records:
    print(record)

print(f"\nGenerated {len(records)} transactions.")
```
