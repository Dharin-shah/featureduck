# FeatureDuck Roadmap

> To create GitHub issues: `gh auth login` then run the commands at the bottom of this file.

## High Priority

### 1. Postgres/MySQL as Data Sources
**Status:** Planned | **Effort:** Medium (2-3 days)

Add support for reading from Postgres and MySQL using DuckDB scanner extensions.

```python
events = DataSource(
    name="events",
    source_type="postgres",  # or "mysql"
    connection_string="host=localhost dbname=analytics",
    schema="public",
    table="events",
    timestamp_field="updated_at",
    incremental=True
)
```

**Implementation:**
- [ ] Extend `DataSource` with postgres/mysql fields
- [ ] Generate `postgres_scan()`/`mysql_scan()` SQL
- [ ] Install DuckDB extensions on init
- [ ] Incremental watermark tracking
- [ ] Tests with local databases

---

### 2. Multi-Table Online Store
**Status:** Planned | **Effort:** Medium (1-2 days)

Replace single `online_features` table with per-feature-view tables.

**Current (Problem):**
```sql
CREATE TABLE online_features (
    feature_view TEXT,
    entity_key TEXT,
    features JSONB,
    PRIMARY KEY (feature_view, entity_key)
);
```

**Proposed:**
```sql
CREATE TABLE online_{feature_view} (
    entity_key TEXT PRIMARY KEY,
    features JSONB,  -- or typed columns
    updated_at TIMESTAMPTZ
);
```

**Benefits:** Better partitioning, per-view indexes, no contention

---

### 3. Databricks Unity Catalog Support
**Status:** Planned | **Effort:** High (3-5 days)

Read from Unity Catalog tables for feature computation.

**Options:**
1. **DuckDB + Delta Lake** - UC tables are Delta under the hood
   ```sql
   SELECT * FROM delta_scan('s3://uc-bucket/catalog/schema/table')
   ```
2. **Spark Connect** - Execute on Databricks cluster
3. **Databricks SQL Connector** - Direct SQL via warehouse

**Implementation:**
- [ ] Add `source_type="unity_catalog"` to DataSource
- [ ] Handle UC authentication (OAuth, PAT)
- [ ] Support `catalog.schema.table` naming
- [ ] Integration tests with Databricks

---

### 4. Production Benchmarks
**Status:** Planned | **Effort:** Medium (2-3 days)

Run production-level benchmarks and document best practices.

**Scenarios:**
- [ ] Write throughput: 1M, 10M, 100M rows to Delta
- [ ] Read latency: P50/P95/P99 for 1, 100, 10K entities
- [ ] Online store: Redis vs Postgres latency
- [ ] E2E pipeline: Source → Transform → Delta → Online

**Deliverables:**
- [ ] Benchmark scripts in `benchmarks/`
- [ ] BENCHMARK.md with results
- [ ] Production deployment recommendations

---

## Medium Priority

### 5. Spark Engine Support
**Effort:** High (5-7 days)

Add SparkCompiler for LogicalPlan → Spark SQL.

---

### 6. REST API Completion
**Effort:** Low (1 day)

- `GET /features/{view}/{entity}`
- `POST /features/{view}/batch`

---

## Low Priority

### 7. Iceberg/Hudi Support
### 8. Feature Freshness Monitoring
### 9. A/B Testing Integration

---

## Create GitHub Issues

After `gh auth login`, run:

```bash
# Issue 1: Postgres/MySQL sources
gh issue create --repo Dharin-shah/featureduck \
  --title "Add Postgres/MySQL as data sources" \
  --label "enhancement" \
  --body "DuckDB scanner extensions for postgres_scan/mysql_scan with incremental watermarks"

# Issue 2: Multi-table online store
gh issue create --repo Dharin-shah/featureduck \
  --title "Multi-table online store (one table per feature view)" \
  --label "enhancement" \
  --body "Replace single online_features table with per-view tables for better performance"

# Issue 3: Databricks Unity Catalog
gh issue create --repo Dharin-shah/featureduck \
  --title "Databricks Unity Catalog support" \
  --label "enhancement" \
  --body "Read from Unity Catalog tables via DuckDB delta_scan or Spark Connect"

# Issue 4: Production benchmarks
gh issue create --repo Dharin-shah/featureduck \
  --title "Production benchmarks and data loading strategy" \
  --label "documentation" \
  --body "Benchmark write throughput, read latency, and E2E pipeline performance"
```
