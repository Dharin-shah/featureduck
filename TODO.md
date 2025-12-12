# FeatureDuck Roadmap

> All items tracked as GitHub issues: https://github.com/Dharin-shah/featureduck/issues

## GitHub Issues

### Core Infrastructure
| # | Title | Priority | Effort |
|---|-------|----------|--------|
| [#1](https://github.com/Dharin-shah/featureduck/issues/1) | Multi-table online store | High | 1-2 days |
| [#2](https://github.com/Dharin-shah/featureduck/issues/2) | Databricks Unity Catalog support | High | 3-5 days |
| [#3](https://github.com/Dharin-shah/featureduck/issues/3) | Production benchmarks | High | 2-3 days |
| [#4](https://github.com/Dharin-shah/featureduck/issues/4) | Spark engine support | Medium | 5-7 days |
| [#5](https://github.com/Dharin-shah/featureduck/issues/5) | REST API completion | Medium | 1 day |
| [#6](https://github.com/Dharin-shah/featureduck/issues/6) | Iceberg/Hudi support | Low | 2-3 days |
| [#7](https://github.com/Dharin-shah/featureduck/issues/7) | Feature freshness monitoring | Low | 2 days |
| [#8](https://github.com/Dharin-shah/featureduck/issues/8) | A/B testing & versioning | Low | 3-5 days |

### LLM & AI Features (Statsig-inspired)
| # | Title | Priority | Effort |
|---|-------|----------|--------|
| [#9](https://github.com/Dharin-shah/featureduck/issues/9) | LLM context & RAG support | Medium | 5-7 days |
| [#10](https://github.com/Dharin-shah/featureduck/issues/10) | LLM evaluation datasets & gold labels | Medium | 3-4 days |
| [#11](https://github.com/Dharin-shah/featureduck/issues/11) | Global/shared datasets | Medium | 2-3 days |
| [#12](https://github.com/Dharin-shah/featureduck/issues/12) | LangChain/LlamaIndex/DSPy integrations | Low | 3-5 days |
| [#14](https://github.com/Dharin-shah/featureduck/issues/14) | Prompt management (versioned prompts) | High | 2-3 days |
| [#15](https://github.com/Dharin-shah/featureduck/issues/15) | LLM-as-a-Judge (automated grading) | High | 3-5 days |
| [#16](https://github.com/Dharin-shah/featureduck/issues/16) | Online Evals (production monitoring) | Medium | 5-7 days |

---

## Architecture Overview

### AI Evals Stack (inspired by Statsig)

```
┌─────────────────────────────────────────────────────────────┐
│                    PROMPT MANAGEMENT                        │
│  • Versioned prompts with model config                     │
│  • Runtime retrieval via SDK                               │
│  • Promotion workflow (staging → live)                     │
└──────────────────────────┬──────────────────────────────────┘
                           │
           ┌───────────────┼───────────────┐
           ▼               ▼               ▼
┌──────────────────┐ ┌─────────────┐ ┌──────────────────┐
│  OFFLINE EVALS   │ │   GRADERS   │ │   ONLINE EVALS   │
│                  │ │             │ │                  │
│ • Fixed test set │ │ • LLM Judge │ │ • Live traffic   │
│ • Gold labels    │ │ • Heuristics│ │ • No ground truth│
│ • Pre-deployment │ │ • Custom    │ │ • Shadow running │
└────────┬─────────┘ └──────┬──────┘ └────────┬─────────┘
         │                  │                  │
         └──────────────────┼──────────────────┘
                            ▼
┌─────────────────────────────────────────────────────────────┐
│                   METRICS & DASHBOARDS                      │
│  • Quality scores over time                                │
│  • Drift detection & alerting                              │
│  • Shadow vs Live comparison                               │
└─────────────────────────────────────────────────────────────┘
```

### Data Sources

```
┌─────────────────────────────────────────────────────────────┐
│                      DATA SOURCES                           │
├─────────────────┬──────────────────┬───────────────────────┤
│ Batch Sources   │ Streaming (CDC)  │ Cloud Warehouses      │
├─────────────────┼──────────────────┼───────────────────────┤
│ • Delta Lake    │ • Postgres CDC   │ • Databricks Unity    │
│ • Parquet       │ • MySQL CDC      │ • Snowflake           │
│ • Iceberg       │ • Kafka          │ • BigQuery            │
│ • Hudi          │                  │                       │
└─────────────────┴──────────────────┴───────────────────────┘
                            │
                            ▼
┌─────────────────────────────────────────────────────────────┐
│                    DuckDB Query Engine                      │
│  • postgres_scan() / mysql_scan()                          │
│  • delta_scan() / iceberg_scan()                           │
│  • read_parquet() / read_csv()                             │
└─────────────────────────────────────────────────────────────┘
```

---

## Quick Links

- **Repository**: https://github.com/Dharin-shah/featureduck
- **Issues**: https://github.com/Dharin-shah/featureduck/issues
- **Discussions**: https://github.com/Dharin-shah/featureduck/discussions
