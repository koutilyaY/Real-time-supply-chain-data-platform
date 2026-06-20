# Implementation status vs. the spec

Maps the guide's phases/instructions to what exists in this repo.

| # | Instruction | Status | Where |
|---|---|---|---|
| 1 | Summarize spec | ✅ | top of `README.md` |
| 2 | Repo + Docker Compose (MinIO, Kafka, Flink, Trino, Dagster, Superset, Prom/Grafana, MLflow, Qdrant, …) | ✅ slice + profiles | `docker-compose.yml` |
| 3 | Ingestion (Airbyte/Debezium → Kafka, schemas/contracts) | ✅ synthetic generator + JSON contracts; **Debezium CDC** (erp-db → Kafka, verified); 🟡 Airbyte documented | `ingestion/` |
| 4 | Flink streaming (windows, joins, curated → Iceberg) | ✅ 4 domains, windows + DLQ | `infra/flink/sql/` |
| 5 | Iceberg Bronze/Silver/Gold + dbt models | ✅ | `lakehouse/`, `transformations/dbt/` |
| 6 | Dagster orchestration + CI/CD | ✅ assets + schedule; GitHub Actions | `orchestration/`, `.github/` |
| 7 | DataHub + knowledge graph | 🟡 documented (own stack); dbt lineage available | `docs/architecture.md` |
| 8 | ML: forecasting, anomaly, supplier risk + MLflow | ✅ runnable scripts + MLflow server | `ml/` |
| 9 | RAG (sentence-transformers + Qdrant + Ollama) | ✅ ingest + ask API | `rag/` |
| 10 | Digital twin (SimPy + Monte Carlo) | ✅ | `digital_twin/` |
| 11 | FastAPI endpoints | ✅ inventory/shipments/forecasts/carriers/iot/chat | `serving/api/` |
| 12 | Superset dashboards | ✅ service up, Trino "Lakehouse" auto-registered (build charts in UI) | `infra/superset/` |
| 13 | Observability (Prom/Grafana/Loki) | ✅ Prometheus (api+minio+flink targets all up) + Grafana; 🟡 Loki documented | `infra/prometheus`, `infra/grafana` |
| 14 | Security/governance (RBAC, masking, audit) | 🟡 policies + masking macros; enforcement points noted | `governance/` |
| 15 | Chaos engineering + DLQ/retries | ✅ run + verified; recovery confirmed, gap found & fixed (restart policies) | `tests/chaos/`, `tests/chaos/FINDINGS.md` |
| 16 | Documentation | ✅ | `README.md`, `docs/` |

Legend: ✅ implemented & runnable · 🟡 partial/scaffolded with a clear next step.

## Suggested next passes
1. **Connectors:** stand up Airbyte OSS + a Debezium Postgres source writing to
   the same topics; retire/augment the generator.
2. **Catalog:** add DataHub as its own compose file; ingest from Trino + dbt
   `manifest.json` for lineage; layer a knowledge graph on top.
3. **Loki:** add the Loki + Promtail services and ship container logs.
4. **Trino auth:** enable file-based access control per `governance/policies/rbac.md`.
5. **Agentic orchestration:** event-driven "ticket" workflows (Temporal/Prefect)
   off the DLQ + alert streams, with guardrails.
