# Tecton API Research

Based on Tecton's documentation, here's how their API works:

## Tecton Batch Feature View Example

```python
from tecton import batch_feature_view, Entity, BatchSource
from datetime import timedelta

# Define entity
user = Entity(name="user", join_keys=["user_id"])

# Define data source
transactions = BatchSource(
    name="transactions",
    batch_config=SnowflakeConfig(
        database="prod",
        schema="analytics",
        table="transactions"
    )
)

# Define feature view with transformation
@batch_feature_view(
    description="User transaction metrics over 1, 3, and 7 days",
    sources=[transactions],
    entities=[user],
    mode="pandas",  # or "spark" or "snowflake"
    batch_schedule=timedelta(days=1),
    timestamp_field="timestamp",
    ttl=timedelta(days=30)
)
def user_transaction_metrics(transactions):
    """
    Transformation function (PySpark or Pandas)
    """
    return transactions.groupBy("user_id").agg(
        count("*").alias("transaction_count_1d"),
        avg("amount").alias("avg_transaction_1d"),
        sum("amount").alias("total_amount_1d")
    )
```

## Tecton Key Patterns

1. **Decorator-based API**: `@batch_feature_view`, `@stream_feature_view`, `@realtime_feature_view`
2. **Transformation modes**: `pandas`, `spark`, `snowflake`
3. **DataFrame API**: Uses PySpark or Pandas DataFrame operations
4. **Built-in time handling**: `timestamp_field`, `ttl`, `batch_schedule`
5. **Entity-based**: Features grouped by entities (user, merchant, etc.)

## What Users Like About Tecton

- Clean decorator syntax
- Familiar PySpark/Pandas API
- No SQL strings (type-safe transformations)
- Auto-backfill and scheduling
- Built-in time-travel

## What We Should Learn From Tecton

✅ Use decorators for feature views
✅ Support both DataFrame and SQL transformations
✅ Make time handling easy (days_ago, time windows)
✅ Type-safe (catch errors at definition time)
✅ Entity-centric design
