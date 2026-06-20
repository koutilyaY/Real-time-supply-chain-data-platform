# Real-Time Supply-Chain Data Platform

A **fully open-source, event-driven** data platform for real-time visibility
across inventory, orders, shipments, suppliers, and IoT sensors. No managed or
paid services — everything runs locally via Docker Compose.

> **Spec summary (alignment).** The goal is unified, streaming ingestion from
> ERP/WMS/TMS/CRM/IoT/external feeds → processed in real time → stored in an
> open **lakehouse** (Iceberg on MinIO) → transformed (dbt) and modeled in
> Bronze/Silver/Gold → orchestrated (Dagster) with CI/CD → enriched with ML
> (forecasting, anomaly detection, supplier risk), a local **RAG** layer, and a
> **digital-twin** simulator → served via **APIs + dashboards**, with metadata,
> data quality, observability, governance, and chaos-tested resilience. See
> `docs/roadmap.md` for the spec-by-spec status.

## What's implemented now (runnable vertical slice)

```
synthetic generator → Kafka → Flink SQL (validate · window · DLQ)
   → Iceberg Bronze/Silver on MinIO → dbt → Iceberg Gold → Trino
   → FastAPI + Superset, observed by Prometheus/Grafana, orchestrated by Dagster.
```

ML, RAG, and the digital twin are runnable as optional profiles/scripts; DataHub,
Loki, Airbyte/Debezium, and Trino auth are scaffolded with clear next steps.

See **`docs/architecture.md`** for the diagram and **`docs/runbook.md`** for URLs
and operations.

## Quick start

Requires Docker + Docker Compose. First build pulls several images and compiles a
custom Flink image (a few minutes).

```bash
make up          # core slice
make lake-init   # create iceberg.{bronze,silver,gold}
make flink-jobs  # submit the 4 streaming jobs
sleep 90         # let 1-min windows close
make dbt-run     # build Gold marts   (pip install dbt-trino first)
make smoke       # end-to-end check
```

Then explore:
- API: http://localhost:8000/docs (`/revenue/hourly`, `/carriers`, `/inventory/health`, …)
- Trino: `make trino-cli` → `SELECT * FROM iceberg.gold.fct_revenue_hourly;`
- Flink jobs: http://localhost:8081 · MinIO: http://localhost:9001

Add layers:
```bash
make bi                      # Superset (http://localhost:8088, admin/admin)
make obs                     # Prometheus + Grafana (http://localhost:3001)
docker compose --profile ml up -d    # MLflow (http://localhost:5000)
docker compose --profile rag up -d   # Qdrant + Ollama + rag-api
make up-full                 # everything
```

## Repository layout

```
docker-compose.yml          # all services (core + profiled layers)
Makefile                    # up / demo / dbt-run / smoke / chaos / validate
ingestion/                  # synthetic generator + JSON data contracts
streaming/  infra/flink/    # Flink SQL streaming jobs (+ custom image)
lakehouse/                  # Iceberg reference DDL
transformations/dbt/        # dbt Core: Silver staging + Gold marts (Trino)
orchestration/dagster/      # Dagster assets + schedule
ml/                         # MLflow server + forecasting/anomaly/supplier-risk
rag/                        # embeddings (Qdrant) + local LLM (Ollama) Q&A
digital_twin/               # SimPy + Monte Carlo what-if simulation
serving/api/                # FastAPI over Trino (+ Prometheus /metrics)
infra/                      # trino, flink, superset, prometheus, grafana configs
governance/                 # RBAC, masking, data-contract policies
tests/                      # integration smoke test + chaos experiments
docs/                       # architecture, runbook, roadmap
.github/workflows/          # CI: compose + python + dbt parse validation
scripts/                    # lake_init, submit_flink_jobs, db bootstrap
```

## Design choices
- **Open formats over engines:** Iceberg tables in MinIO are the source of truth;
  Flink, Trino, dbt, and Spark can all read/write the same warehouse.
- **Graceful degradation:** the API never 500s on a missing table — it returns a
  `note`, so it's usable the moment the stack is up.
- **DLQ + checkpoints:** ~3% of generated events are malformed on purpose; Flink
  routes them to `*.dlq` while valid rows land in Bronze. Chaos tests verify
  recovery.
- **Profiles:** the core slice stays lean; heavier layers (BI/ML/RAG/obs) opt in.

## Validate without starting Docker
```bash
make validate   # docker compose config -q + python compileall
```

## License & tooling
All components are open-source (Apache-2.0 / AGPL where noted): Apache Kafka,
Apache Flink, Apache Iceberg, Trino, dbt Core, Dagster, Apache Superset, MinIO,
Qdrant, Ollama, Prometheus, Grafana, MLflow, SimPy, sentence-transformers.
