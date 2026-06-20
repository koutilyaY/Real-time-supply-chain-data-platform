# Implementation status vs. the spec

Maps the guide's phases/instructions to what exists in this repo.

| # | Instruction | Status | Where |
|---|---|---|---|
| 1 | Summarize spec | ✅ | top of `README.md` |
| 2 | Repo + Docker Compose (MinIO, Kafka, Flink, Trino, Dagster, Superset, Prom/Grafana, MLflow, Qdrant, …) | ✅ slice + profiles | `docker-compose.yml` |
| 3 | Ingestion (Airbyte/Debezium → Kafka, schemas/contracts) | ✅ Avro generator + **Apicurio schema registry**; **Debezium CDC** (verified); JSON contracts in CI; 🟡 Airbyte documented | `ingestion/` |
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

## Tier-1 correctness (added 2026-06-19)
- **Silver dedup** — staging models keep one row per window grain (proven by `dbt_utils.unique_combination_of_columns` tests); at-least-once stream replay no longer double-counts Gold.
- **Richer DQ** — `dbt-expectations` ranges, `accepted_values`, grain-uniqueness on marts, and **source freshness** SLAs. dbt build now PASS=33.
- **Contract enforcement** — `tests/contracts/validate_contracts.py` verifies clean events conform and malformed events are rejected; wired into CI (`contracts` job) + `make contracts`.
- **Schema Registry + Avro** — DONE: Apicurio registry (Confluent-compat), generator produces Avro (schemas auto-registered as `<topic>-value`), all 4 Flink sources use `avro-confluent`. Types enforced at the wire; nullable schemas keep the DLQ for business rules. Verified: Bronze/Silver flow on Avro, dbt PASS=33, DLQ intact.

## Tier-2 lakehouse hygiene (added 2026-06-19)
- **Iceberg maintenance** — DONE: `scripts/iceberg_maintain.sh` + Dagster `iceberg_maintenance` asset (daily 03:30) run `optimize` (compaction), `expire_snapshots`, `remove_orphan_files` over all Bronze/Silver tables. Verified: silver 62→1 files, bronze 344→5 files, snapshots 368→11; data integrity preserved. Prod-safe 7d retention (job lowers per-session only).
- **Still open (Tier-2):** table partitioning + sort order; Flink HA + savepoints; Kafka RF≥2 + named volume; scheduled `pg_dump` + MinIO versioning backups.

## Suggested next passes
1. **Connectors:** stand up Airbyte OSS + a Debezium Postgres source writing to
   the same topics; retire/augment the generator.
2. **Catalog:** add DataHub as its own compose file; ingest from Trino + dbt
   `manifest.json` for lineage; layer a knowledge graph on top.
3. **Loki:** add the Loki + Promtail services and ship container logs.
4. **Trino auth:** enable file-based access control per `governance/policies/rbac.md`.
5. **Agentic orchestration:** event-driven "ticket" workflows (Temporal/Prefect)
   off the DLQ + alert streams, with guardrails.
