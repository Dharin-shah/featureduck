# FeatureDuck Benchmarks

This directory contains all performance benchmarks and their results.

## Structure

```
benchmarks/
├── README.md              ← You are here
├── results/               ← Actual benchmark results (timestamped)
│   └── .gitkeep
├── scripts/               ← Benchmark automation scripts
│   └── .gitkeep
└── comparison/            ← Comparative benchmarks (vs Spark, Tecton)
    └── .gitkeep
```

## Running Benchmarks

### Microbenchmarks (Criterion)
```bash
cargo bench --bench feature_serving
cargo bench --bench delta_read_latency
```

### Load Tests (k6)
```bash
k6 run tests/load/online_serving.js
```

### Comparative Benchmarks
```bash
make benchmark-vs-spark
```

## Results Format

Each benchmark result must include:
1. **Purpose:** What are we measuring?
2. **Setup:** Hardware, data size, config
3. **Method:** Exact commands to reproduce
4. **Results:** Raw numbers with timestamp
5. **Analysis:** What it means, next steps

See FEATUREDUCK.md "Testing & Benchmarking Strategy" for details.

## Current Status

| Benchmark | Status | Target | Actual | Date |
|-----------|--------|--------|--------|------|
| DuckLake read latency | ⏳ Not run | < 50ms P99 | TBD | M1 |
| Delta Lake read latency | ⏳ Not run | < 50ms P99 | TBD | M1 |
| Online serving throughput | ⏳ Not run | 10K RPS/pod | TBD | M2 |
| Write throughput | ⏳ Not run | 1 TB/hour | TBD | M2 |
| DuckDB vs Spark | ⏳ Not run | 5-10x faster | TBD | M5 |

**All benchmarks will be run and documented as we build each milestone.**
