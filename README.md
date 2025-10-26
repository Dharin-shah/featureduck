# FeatureDuck 🦆

**Universal Feature Store powered by DuckDB**

[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](LICENSE)
[![Rust](https://img.shields.io/badge/rust-1.75%2B-orange.svg)](https://www.rust-lang.org)

> **Status:** Milestone 0 - Foundation ✅

FeatureDuck is a simple, cost-effective feature store that leverages DuckDB for compute and supports any lakehouse backend. Built with Rust for performance and Python for accessibility.

## 🎯 Goals

- **10-50x cheaper** than commercial alternatives ($7K-40K vs $100K-380K annually)
- **Sub-10ms latency** for online feature serving
- **Universal lakehouse support** (Delta Lake, Iceberg, Hudi, DuckDB native)
- **Perfect online/offline parity** (single source of truth)
- **30-minute deployment** (one command: `helm install`)

## 🏗️ Architecture

```
┌─────────────────────────────────┐
│   Rust Server (Axum + DuckDB)  │  ← Fast, efficient, single binary
└────────────────┬────────────────┘
                 │
┌────────────────▼────────────────┐
│   Any Lakehouse Format          │  ← Delta Lake, Iceberg, Hudi
│   (S3/GCS/Azure/Local)          │
└─────────────────────────────────┘
```

**Key Principles:**
- **KISS:** Keep It Simple, Stupid
- **TDD:** Test-Driven Development
- **Single Source of Truth:** Lakehouse-only storage (no separate online store)
- **Well-Commented:** Every non-obvious line explained

## 🚀 Quick Start

### Prerequisites

- Rust 1.75+ ([install](https://rustup.rs))
- Python 3.10+ (for SDK)

### Build & Run

```bash
# Clone repository
git clone https://github.com/featureduck/featureduck
cd featureduck

# Build server
cargo build --release

# Run server (Milestone 0: mock data)
cargo run --bin featureduck serve

# Server starts on http://localhost:8000
```

### Test the API

```bash
# Health check
curl http://localhost:8000/health

# Get online features (mock data in Milestone 0)
curl -X POST http://localhost:8000/v1/features/online \
  -H "Content-Type: application/json" \
  -d '{
    "feature_view": "user_features",
    "entities": [{"user_id": "123"}]
  }'
```

## 📊 Current Status: Milestone 0

**Milestone 0: Foundation** ✅
- [x] Rust workspace with 3 crates (core, server, delta)
- [x] Core types (EntityKey, FeatureView, FeatureRow)
- [x] StorageConnector trait (interface for lakehouse backends)
- [x] HTTP API with Axum (health check, mock endpoints)
- [x] Error handling and logging
- [x] Comprehensive comments for Rust beginners
- [x] Tests for all modules

**Next:** Milestone 1 - Delta Lake Connector (TDD)

## 🗺️ Roadmap

| Milestone | Goal | Status |
|-----------|------|--------|
| **M0** | Foundation | ✅ Complete |
| **M1** | Delta Lake connector + DuckDB integration | 🔨 In Progress |
| **M2** | Feature serving (online + historical) | ⏳ Planned |
| **M3** | REST API + Python SDK | ⏳ Planned |
| **M4** | Docker + CLI | ⏳ Planned |
| **M5** | Performance optimization (< 10ms P99) | ⏳ Planned |
| **M6** | Universal lakehouse (Iceberg, Hudi) | ⏳ Planned |

## 🧪 Testing

```bash
# Run all tests
cargo test

# Run with coverage
cargo install cargo-tarpaulin
cargo tarpaulin --out Html

# Run specific crate tests
cargo test -p featureduck-core
cargo test -p featureduck-server
```

## 📚 Documentation

- [Architecture Principles](ARCHITECTURE_PRINCIPLES.md) - Core design decisions
- [Tech Stack](TECH_STACK.md) - Why Rust + Python
- [Feature Store Components](FEATURE_STORE_COMPONENTS.md) - Complete breakdown
- [Competitive Analysis](COMPETITIVE_ANALYSIS.md) - vs Tecton, Feast, etc.
- [Caching Strategy](CACHING_STRATEGY.md) - Why no Redis

## 🤝 Contributing

We follow strict TDD and code quality practices:

1. **Write tests first** (Red → Green → Refactor)
2. **Comment extensively** (assume reader is Rust beginner)
3. **Keep it simple** (KISS principle, YAGNI)
4. **Run checks:**
   ```bash
   cargo clippy -- -D warnings
   cargo fmt --check
   cargo test
   ```

## 📝 License

Apache 2.0 - See [LICENSE](LICENSE) for details.

## 🙏 Acknowledgments

Inspired by:
- **Tecton:** Declarative feature definitions
- **Feast:** Open-source community approach
- **DuckDB:** Incredible in-process query engine

Built with ❤️ to make feature stores accessible to everyone.

---

**Status:** Milestone 0 Complete | **Next:** Implement Delta Lake connector with TDD
