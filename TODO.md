# FeatureDuck Roadmap

## High Priority

### 1. Postgres/MySQL as Data Sources
**Status:** Planned
**Effort:** Medium (2-3 days)

Add support for reading from Postgres and MySQL as feature sources using DuckDB scanner extensions.

**Approach:**
```python
# Postgres source
events = DataSource(
    name="events",
    source_type="postgres",
    connection_string="host=localhost dbname=analytics",
    schema="public",
    table="events",
    timestamp_field="updated_at",
    incremental=True  # Use timestamp for incremental reads
)

# MySQL source
events = DataSource(
    name="events",
    source_type="mysql",
    connection_string="host=localhost database=analytics",
    table="events",
    timestamp_field="updated_at"
)
```

**Generated SQL:**
```sql
-- Postgres
SELECT * FROM postgres_scan(
  'host=localhost dbname=analytics',
  'public', 'events'
) WHERE updated_at > $last_watermark

-- MySQL
SELECT * FROM mysql_scan(
  'host=localhost database=analytics',
  'events'
) WHERE updated_at > $last_watermark
```

**Implementation:**
- [ ] Extend `DataSource` class with postgres/mysql fields
- [ ] Add connection_string, schema, table fields
- [ ] Generate postgres_scan/mysql_scan SQL in SQLFrame
- [ ] Install DuckDB postgres/mysql extensions on init
- [ ] Add incremental watermark tracking
- [ ] Tests with local Postgres/MySQL

**Future (CDC):**
- Debezium for true real-time WAL/binlog capture
- Kafka integration for streaming updates

---

### 2. Multi-Table Online Store
**Status:** Planned
**Effort:** Medium (1-2 days)

Current single-table design is suboptimal:
```sql
-- Current: One table for all feature views
CREATE TABLE online_features (
    feature_view TEXT NOT NULL,
    entity_key TEXT NOT NULL,
    features JSONB NOT NULL,
    PRIMARY KEY (feature_view, entity_key)
);
```

**Problems:**
- All feature views share one table (contention, bloat)
- Can't optimize indexes per feature view
- Can't partition/shard per feature view
- JSONB for all features (no type-specific optimization)

**Proposed: One table per feature view**
```sql
-- Table per feature view with typed columns
CREATE TABLE online_user_features (
    entity_key TEXT PRIMARY KEY,
    purchase_count BIGINT,
    total_spent DOUBLE PRECISION,
    avg_order_value DOUBLE PRECISION,
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

-- Or with JSONB but isolated
CREATE TABLE online_{feature_view} (
    entity_key TEXT PRIMARY KEY,
    features JSONB NOT NULL,
    updated_at TIMESTAMPTZ DEFAULT NOW()
);
```

**Implementation:**
- [ ] Add `create_feature_table(feature_view, schema)` to OnlineStore trait
- [ ] Generate table name from feature view: `online_{feature_view}`
- [ ] Support typed columns (from feature schema) or JSONB fallback
- [ ] Update read/write to route to correct table
- [ ] Migration path for existing single-table users
- [ ] Tests for multi-table operations

**Benefits:**
- Better partitioning and sharding
- Per-view index optimization
- Easier maintenance (VACUUM per table)
- Better cache utilization
- Can use native Postgres types instead of JSONB

---

## Medium Priority

### 3. Spark Engine Support
**Status:** Planned
**Effort:** High (5-7 days)

Add SparkCompiler for generating Spark SQL from LogicalPlan.

**Key Differences from DuckDB:**
- Data sources: `spark.read.parquet()` vs `read_parquet()`
- Intervals: `INTERVAL 7 DAYS` vs `INTERVAL '7' DAYS`
- QUALIFY: Subquery with window function
- Functions: `date_sub()` vs arithmetic

---

### 4. REST API for Feature Serving
**Status:** Partial
**Effort:** Low (1 day)

Complete HTTP API for online feature serving:
- `GET /features/{view}/{entity}` - Get features for entity
- `POST /features/{view}/batch` - Batch get features
- Health checks already implemented

---

## Low Priority

### 5. Iceberg/Hudi Support
Add alternative table formats via DuckDB extensions.

### 6. Feature Freshness Monitoring
Track feature staleness and alert on stale features.

### 7. A/B Testing Integration
Support feature versioning for experiments.
