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
from featureduck import feature_view, Entity, DeltaSource
from featureduck import col, count, sum_, days_ago

user = Entity(name="user", join_keys=["user_id"])
events = DeltaSource(name="events", path="s3://bucket/events")

@feature_view(name="user_features", source=events, entities=[user])
def compute(df):
    return (
        df.filter(col("timestamp") >= days_ago(7))
          .group_by("user_id")
          .agg(count().alias("purchases_7d"), sum_("amount").alias("total_spent"))
    )
```

### SQLFrame API

```python
from featureduck import FeatureView, F

view = FeatureView(name="user_features", entities=["user_id"], engine="duckdb")
df = view.read_source({"type": "parquet", "path": "s3://bucket/events"})
features = df.groupBy("user_id").agg(F.count("*").alias("event_count"))
view.materialize(features, output_path="s3://bucket/features")
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
# All lib tests (245 passing)
cargo test --workspace --lib --quiet

# Stress tests
cargo test -p featureduck-delta --test stress_test_optimizations --release
```

## License

Apache 2.0
