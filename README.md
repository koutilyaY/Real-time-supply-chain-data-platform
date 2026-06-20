<div align="center">

# 🛰️ Real-Time Supply-Chain Data Platform

### An open-source, event-driven lakehouse for up-to-the-second visibility across inventory, orders, shipments, suppliers, and IoT — streaming, ML, RAG, and a digital twin, with production-grade hardening. No managed or paid services.

<br>

[![Status](https://img.shields.io/badge/status-running%20%26%20verified-2ea44f?style=flat-square)](#-capability-matrix)
[![Stack](https://img.shields.io/badge/stack-100%25%20open--source-1f6feb?style=flat-square)](#-tech-stack)
[![Lakehouse](https://img.shields.io/badge/lakehouse-Apache%20Iceberg-2ea44f?style=flat-square)](#-architecture)
[![Streaming](https://img.shields.io/badge/streaming-Kafka%20%2B%20Flink-e25a1c?style=flat-square)](#-architecture)
[![Serving](https://img.shields.io/badge/serving-Trino%20%2B%20FastAPI-blueviolet?style=flat-square)](#-live-data)
[![Runs locally](https://img.shields.io/badge/runs-Docker%20Compose-2496ED?style=flat-square&logo=docker&logoColor=white)](#-quickstart)

<br>

![Kafka](https://img.shields.io/badge/Apache%20Kafka-231F20?logo=apachekafka&logoColor=white&style=for-the-badge)
![Flink](https://img.shields.io/badge/Apache%20Flink-E6526F?logo=apacheflink&logoColor=white&style=for-the-badge)
![Iceberg](https://img.shields.io/badge/Apache%20Iceberg-1C9BD8?logo=apache&logoColor=white&style=for-the-badge)
![Trino](https://img.shields.io/badge/Trino-DD00A1?logo=trino&logoColor=white&style=for-the-badge)
![dbt](https://img.shields.io/badge/dbt-FF694B?logo=dbt&logoColor=white&style=for-the-badge)
![Dagster](https://img.shields.io/badge/Dagster-654FF0?logo=dagster&logoColor=white&style=for-the-badge)
![MinIO](https://img.shields.io/badge/MinIO-C72E49?logo=minio&logoColor=white&style=for-the-badge)
![MLflow](https://img.shields.io/badge/MLflow-0194E2?logo=mlflow&logoColor=white&style=for-the-badge)
![Docker](https://img.shields.io/badge/Docker-2496ED?logo=docker&logoColor=white&style=for-the-badge)

</div>

---

## ⚡ TL;DR

```bash
make up && make lake-init && make flink-jobs   # boot core + lakehouse + 4 streaming jobs
sleep 90 && make dbt-run                        # let 1-min windows close, then build Gold marts
make smoke                                       # end-to-end check  →  prints "SMOKE OK"
```

Then open the **API** at **[localhost:8000/docs](http://localhost:8000/docs)** and **Flink** at **[localhost:8081](http://localhost:8081)**.
Full walkthrough → **[docs/RUN_GUIDE.md](docs/RUN_GUIDE.md)** · Non-technical overview → **[docs/Platform_Guide_for_Everyone.docx](docs/Platform_Guide_for_Everyone.docx)**

---

## 📖 Table of Contents

[Why](#-why) · [Architecture](#-architecture) · [What you get](#-what-you-get) · [Capability matrix](#-capability-matrix) · [Production hardening](#-production-hardening) · [Tech stack](#-tech-stack) · [Quickstart](#-quickstart) · [Live data](#-live-data) · [Repo layout](#-repository-layout) · [Day-2 ops](#-day-2-operations) · [Design principles](#-design-principles) · [Docs](#-documentation) · [License](#-license)

---

## 🎯 Why

Manufacturers, retailers, and logistics providers run on **batch** data platforms: signals from ERP, WMS, TMS, and IoT arrive hours late and scattered across silos. By the time a late container, a stockout, or a cold-chain excursion is noticed, the damage is already done — and the post-mortem lives in yet another spreadsheet.

This platform replaces that with a **single, live, trustworthy picture** — ingested the instant events happen, validated at the wire, processed in real time, landed in an open lakehouse, and enriched with forecasting, anomaly detection, supplier-risk scoring, a what-if simulator, and a plain-English assistant grounded in your own documents.

> **It runs end-to-end on a laptop with nothing but Docker** — every component is open-source, self-hosted, and free of managed services or per-seat licences.

---

## 🏗️ Architecture

```mermaid
flowchart LR
  subgraph SOURCES["Sources"]
    GEN["Synthetic generator<br/>ERP · WMS · TMS · IoT"]
    CDC["Debezium CDC<br/>operational DB"]
  end

  GEN -- "Avro" --> K
  CDC --> K
  SR["Schema Registry<br/>(Apicurio)"] -. enforces .- K
  K["Apache Kafka<br/>event bus (KRaft)"] --> F["Apache Flink SQL<br/>validate · window · join"]
  F -- "invalid" --> DLQ[("Dead-letter<br/>queue")]
  F -- "valid + aggregates" --> ICE

  subgraph LAKE["Lakehouse — Apache Iceberg on MinIO (S3)"]
    ICE[("Bronze → Silver")]
    GOLD[("Gold marts")]
  end
  REST["Iceberg REST catalog<br/>(Postgres-backed)"] -. catalog .- LAKE

  ICE --> DBT["dbt Core<br/>dedup · tests · masking"] --> GOLD
  GOLD --> TR["Trino<br/>query engine"]
  TR --> API["FastAPI<br/>auth · rate-limit"]
  TR --> BI["Superset<br/>dashboards"]
  GOLD --> ML["MLflow<br/>forecast · anomaly · risk"]
  GOLD --> TWIN["Digital twin<br/>SimPy + Monte Carlo"]
  DOCS["SOPs / policies"] --> RAG["RAG<br/>Qdrant + local LLM"]
  RAG --> API

  DAG["Dagster"] -. orchestrates .-> DBT
  DAG -. schedules .-> ML
  DAG -. maintains .-> LAKE
  API --> PROM["Prometheus → Grafana<br/>metrics + alerts"]
```

**Medallion layers** — `Bronze` (validated raw) → `Silver` (deduped streaming aggregates) → `Gold` (business marts). The Gold layer ships five marts: hourly revenue, carrier performance, inventory health, IoT hourly rollups, and a PII-masked order fact.

---

## 📦 What You Get

| | Capability |
|---|---|
| 📡 | **Real-time ingestion** — Avro events on Kafka with **enforced schemas** (Apicurio registry) plus **Debezium CDC** from an operational Postgres |
| 🌊 | **Stream processing** — Flink SQL across four domains with event-time windows, checkpointing to Iceberg, and a SQL-native **dead-letter queue** for malformed records |
| 🧊 | **Open lakehouse** — Apache Iceberg on MinIO (S3), with a **Postgres-backed REST catalog** for true multi-writer concurrency |
| 🔧 | **Transformations** — dbt Core (Trino adapter) with **Silver dedup**, data-quality tests, `dbt-expectations`, and **source freshness** |
| 🗓️ | **Orchestration** — Dagster with **per-model dbt asset lineage**, data-quality checks, and scheduled lakehouse maintenance |
| 🤖 | **ML** — demand forecasting, IoT anomaly detection, and supplier-risk scoring, tracked in MLflow (artifacts in MinIO) |
| 💬 | **RAG assistant** — embed docs → Qdrant → **local LLM via Ollama**, fully offline; answers grounded in your own documents |
| 🪞 | **Digital twin** — SimPy + Monte Carlo "what-if" simulator (supplier outages, demand spikes, warehouse capacity) |
| 🚪 | **Serving** — FastAPI (API-key auth + rate limiting) over Trino, and Apache Superset dashboards |
| 📈 | **Observability** — Prometheus + Grafana with alert rules (target down, error rate, latency, DLQ) |
| 🔐 | **Governance** — PII masking applied in dbt (sha256), data contracts validated in CI, RBAC + secrets policies |
| 🛡️ | **Resilience** — chaos-tested recovery, restart policies, backups (pg_dump + MinIO versioning) |

---

## ✅ Capability Matrix

Everything below is **built, running, and verified end-to-end** — not a roadmap item:

| Area | Status | Evidence |
|---|:---:|---|
| Ingestion — Avro + schema registry, CDC | ✅ | 5 contracts registered; live UPDATE/INSERT captured via Debezium |
| Streaming — 4 domains, windows, DLQ | ✅ | 4 Flink jobs RUNNING; malformed records routed to DLQ |
| Lakehouse — Bronze / Silver / Gold | ✅ | Avro data flows through all layers; queryable via Trino |
| dbt models + dedup + DQ | ✅ | `dbt build` passes; 4 staging + 5 Gold models; source freshness green |
| Orchestration — dagster-dbt lineage | ✅ | per-model assets materialize; RUN_SUCCESS |
| ML — 3 models → MLflow | ✅ | runs logged; `model.pkl` artifacts stored in MinIO |
| RAG — Qdrant + Ollama | ✅ | grounded answer returned from the local LLM |
| Digital twin | ✅ | Monte Carlo service-level / stockout distributions |
| Serving — API auth + rate limit | ✅ | `401` / `200` / `429` responses verified |
| Observability — alerts | ✅ | Prometheus alert rules loaded (down / error / latency / DLQ) |
| Chaos / resilience | ✅ | killed broker / taskmanager / MinIO → auto-recovered |

> All figures in this README come from **synthetic generator data** (deterministic seed). The numbers are real outputs of the running system — they are not benchmarks or production statistics.

Spec-by-spec detail → **[docs/roadmap.md](docs/roadmap.md)**.

---

## 🛡️ Production Hardening

This isn't a demo that falls over the moment you touch it. It has been deliberately stress-tested and hardened across three tiers.

<details>
<summary><b>Tier 1 — Correctness</b> (click to expand)</summary>

- **Avro wire-level schema enforcement** backed by the Apicurio registry — producers can't ship records that violate the contract.
- **Silver dedup** so at-least-once streaming delivery can never double-count revenue or shipments.
- **dbt data-quality tests + source freshness** on every build; **data contracts** validated in CI.

</details>

<details>
<summary><b>Tier 2 — Lakehouse hygiene</b> (click to expand)</summary>

- Scheduled **file compaction**, **snapshot expiration**, and **orphan-file cleanup** keep Iceberg metadata small and queries fast.
- **Day-partitioning** on the lakehouse tables.
- **Backups** — `pg_dump` of all metadata databases plus MinIO object versioning.

</details>

<details>
<summary><b>Tier 3 — Ops & security</b> (click to expand)</summary>

- **API-key auth + rate limiting** on the serving layer (`401` / `200` / `429` all verified).
- **PII masking** (sha256, salted) applied in dbt so the order fact never exposes raw customer identifiers.
- **Prometheus alert rules** for target-down, error rate, latency, and DLQ depth.
- **dagster-dbt** per-model lineage so every Gold table is an addressable, observable asset.

</details>

**Chaos-tested** — `make chaos` kills the broker, a task manager, and object storage in turn; the durable layers and streaming jobs recover automatically. Findings → **[tests/chaos/FINDINGS.md](tests/chaos/FINDINGS.md)**.

---

## 🧰 Tech Stack

| Layer | Tools |
|---|---|
| **Event bus / registry** | Apache Kafka (KRaft) · Apicurio Schema Registry |
| **CDC** | Debezium (Kafka Connect) |
| **Stream processing** | Apache Flink (SQL) |
| **Lakehouse** | Apache Iceberg · MinIO · Iceberg REST catalog (Postgres-backed) |
| **Transform** | dbt Core (Trino adapter) · dbt-utils · dbt-expectations |
| **Query** | Trino |
| **Orchestration** | Dagster · dagster-dbt |
| **ML / AI** | MLflow · scikit-learn · statsmodels · Qdrant · Ollama · fastembed |
| **Simulation** | SimPy + Monte Carlo |
| **Serving / BI** | FastAPI · Apache Superset |
| **Observability** | Prometheus · Grafana |
| **Infra** | Docker Compose · GitHub Actions |

> Every component is open-source and self-hosted. No vendor lock-in, no managed services, no per-seat licences.

---

## 🚀 Quickstart

**Prerequisite:** Docker Desktop running. Everything else runs in containers.

```bash
cd "Real‑Time Supply‑Chain Data Platform"
make up           # core stack (first run builds images: a few minutes)
make lake-init    # create Bronze / Silver / Gold namespaces + tables
make flink-jobs   # submit the 4 streaming jobs  → http://localhost:8081
sleep 90          # let the first 1-minute windows close
make dbt-run      # build Gold marts (runs in a throwaway dbt container)
make smoke        # end-to-end check → "SMOKE OK"
```

Add optional layers à la carte — each is a Compose profile:

```bash
make bi                                        # Superset dashboards
make obs                                       # Prometheus + Grafana
make cdc                                       # Debezium change-data-capture
docker compose --profile ml up -d              # MLflow
docker compose --profile orchestration up -d   # Dagster
docker compose --profile rag up -d             # Qdrant + Ollama + RAG API
make up-full                                   # everything, all at once
```

> **Tips** — `make demo` chains `up → lake-init → flink-jobs` with the right waits. `make help` lists every command. Default ports are configurable in `.env` (copy from `.env.example`).

---

## 📊 Live Data

The API serves real figures from the Gold marts. Data endpoints require an API key (default `dev-secret-key`, set in `.env`):

```bash
$ curl -s localhost:8000/carriers -H "X-API-Key: dev-secret-key" | jq
{
  "count": 5,
  "data": [
    { "carrier": "UPS", "total_shipments": 772, "delayed_shipments": 411,
      "delay_rate": 0.532, "on_time_rate": 0.468 }, …
  ]
}

$ curl -s "localhost:8000/revenue/hourly?limit=1" -H "X-API-Key: dev-secret-key"
# { "region": "LATAM", "revenue_hour": "…T21:00:00", "orders": 2129, "gross_revenue": 13568003.91 }
```

> Values shown are from synthetic generator data and will differ on each run.

| Service | URL | Login |
|---|---|---|
| API (Swagger) | http://localhost:8000/docs | header `X-API-Key: dev-secret-key` |
| Flink | http://localhost:8081 | — |
| Trino | http://localhost:8080 | — |
| MinIO console | http://localhost:9001 | admin / password |
| Superset | http://localhost:8088 | admin / admin |
| Dagster | http://localhost:3000 | — |
| MLflow | http://localhost:5000 | — |
| Grafana | http://localhost:3001 | anon / admin |

---

## 🗂️ Repository Layout

```
docker-compose.yml        # all services (core + profiled layers)
Makefile                  # up · demo · dbt-run · smoke · maintain · backup · chaos …
ingestion/                # Avro generator + JSON data contracts + Debezium CDC config
infra/flink/sql/          # Flink SQL streaming jobs (windows + DLQ) — 4 domain pipelines
infra/iceberg-rest/       # Postgres-backed Iceberg REST catalog image
transformations/dbt/      # dbt: Silver staging (deduped) + Gold marts + tests + masking
orchestration/dagster/    # dagster-dbt assets, DQ checks, Iceberg maintenance, schedules
ml/                       # MLflow server + forecasting / anomaly / supplier-risk
rag/                      # fastembed → Qdrant → Ollama Q&A API
digital_twin/             # SimPy + Monte Carlo what-if simulator
serving/api/              # FastAPI over Trino (auth, rate limiting, /metrics)
infra/                    # trino, superset, prometheus (+ alerts), grafana configs
governance/               # RBAC, masking, data-contract policies
tests/                    # integration smoke test · contract tests · chaos experiments
scripts/                  # lake_init · submit_flink_jobs · maintenance · partitioning · backup
docs/                     # RUN_GUIDE · architecture · roadmap · runbook · non-tech guide
.github/workflows/        # CI: compose validate + python + contracts + dbt parse
```

---

## 🔧 Day-2 Operations

```bash
make maintain    # compact files + expire snapshots + remove orphans (RETENTION=7d)
make partition   # day-partition the lakehouse tables (then re-run flink-jobs)
make backup      # pg_dump all metadata databases → ./backups
make contracts   # validate generator events against the JSON data contracts
make dq          # dbt tests + source freshness
make chaos       # resilience drill (kills broker / taskmanager / MinIO)
make ps          # show running services
make down        # stop, keep data   ·   make clean = stop + wipe volumes
```

---

## 🧭 Design Principles

- **Open formats over engines.** Iceberg on MinIO is the single source of truth; Flink, Trino, and dbt all read and write the same warehouse — no proprietary table format, no lock-in.
- **Correctness first.** Schemas enforced at the wire, dedup in Silver, quality tests on every build.
- **Fail loud, recover quietly.** Malformed records go to a DLQ; services carry restart policies and recover from checkpoints — proven by the chaos drills, not assumed.
- **Graceful degradation.** The API returns a `note` instead of a 500 when a table isn't ready yet, so it's usable the moment the stack is up.
- **Lean core, opt-in depth.** The core slice is small and fast; BI, ML, RAG, observability, and CDC are profiles you enable only when you need them.

---

## 📚 Documentation

| Doc | For |
|---|---|
| **[docs/RUN_GUIDE.md](docs/RUN_GUIDE.md)** | Step-by-step terminal run guide |
| **[docs/architecture.md](docs/architecture.md)** | Architecture + data flow deep-dive |
| **[docs/roadmap.md](docs/roadmap.md)** | Spec-by-spec status + what's next |
| **[docs/runbook.md](docs/runbook.md)** | URLs, operations, troubleshooting |
| **[tests/chaos/FINDINGS.md](tests/chaos/FINDINGS.md)** | Chaos-test results |
| **[docs/Platform_Guide_for_Everyone.docx](docs/Platform_Guide_for_Everyone.docx)** | Plain-English overview for non-technical readers |

---

## 📄 License

Every component in the stack is open-source and self-hostable, used under its respective upstream license (Apache-2.0, AGPL, and others as noted by each project). Project-level licensing for this repository is at the author's discretion — see the repository's license file, if present, for terms.

---

<div align="center">

**Built with 100% open-source tools — streaming, lakehouse, ML, RAG, digital twin, and production hardening, all running locally.**

[⬆ back to top](#️-real-time-supply-chain-data-platform)

</div>
