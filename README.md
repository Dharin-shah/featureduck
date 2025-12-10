# FeatureDuck

High-performance feature store for ML and LLM applications. Built in Rust, accessible via Python.

## Features

- **Fast**: 45K+ rows/sec writes, <10ms reads via Redis/Postgres online stores
- **Reliable**: Circuit breakers, retry with backoff, self-healing OOM recovery
- **Flexible**: Delta Lake storage, Redis/Postgres online serving
- **Simple**: Decorator-based API, SQLFrame-compatible, single binary deployment

## Quick Start

```bash
# Build
cargo build --release

# Run tests
cargo test --workspace --lib

# Python SDK
pip install -e python/
```

## Usage

### Decorator API

```python
from featureduck import FeatureDefinition, Entity, DataSource
from sqlframe.duckdb import functions as F

user = Entity(name="user", join_keys=["user_id"])
events = DataSource(name="events", source_type="delta", path="s3://bucket/events")

@FeatureDefinition(name="user_features", entities=[user], source=events, ttl_days=7)
def user_purchases(df):
    return (
        df.filter(F.col("event_type") == "purchase")
          .groupBy("user_id")
          .agg(
              F.count("*").alias("purchase_count"),
              F.sum("amount").alias("total_spent")
          )
    )

# Register and materialize
user_purchases.apply()
```

### SQLFrame API

```python
from featureduck import FeatureView, F

view = FeatureView(name="user_features", entities=["user_id"], engine="duckdb")
df = view.read_source({"type": "parquet", "path": "s3://bucket/events"})
features = df.groupBy("user_id").agg(F.count("*").alias("event_count"))
view.materialize(features, output_path="s3://bucket/features")
```

### Validation

```python
from featureduck.validation import FeatureView, assert_type, assert_range, reject_pii

@FeatureView(name="user_features", entities=["user_id"])
@assert_type("age", int)
@assert_range("age", min=0, max=150)
@reject_pii(["ssn", "credit_card"])
class UserFeatures:
    pass

# Validate a feature row
schema = UserFeatures._feature_view_schema
is_valid, error = schema.validate({"entities": [{"name": "user_id", "value": "123"}], "features": {"age": 25}})
```

## Architecture

```
crates/
├── featureduck-core/     # Core types, traits, validation
├── featureduck-delta/    # Delta Lake storage connector
├── featureduck-online/   # Redis + Postgres online stores
├── featureduck-registry/ # Feature registry (SQLite/Postgres)
├── featureduck-server/   # HTTP API server
├── featureduck-cli/      # Command line tool
└── featureduck-py/       # PyO3 Python bindings

python/
└── featureduck/          # Python SDK
```

## Tests

```bash
# All lib tests
cargo test --workspace --lib --quiet

# Stress tests
cargo test -p featureduck-delta --test stress_test_optimizations --release
```

## License

Apache 2.0
