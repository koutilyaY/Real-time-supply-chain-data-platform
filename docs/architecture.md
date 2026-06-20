# Architecture

## Data flow (vertical slice — implemented)

```mermaid
flowchart LR
  subgraph Sources
    GEN[Synthetic generator<br/>ERP/WMS/TMS/IoT events]
  end
  GEN -->|JSON events| K[(Kafka<br/>orders, inventory,<br/>shipments, iot.sensors)]
  K --> F[Flink SQL jobs<br/>validate · window · join]
  F -->|valid| BRONZE[(Iceberg Bronze<br/>*_raw)]
  F -->|aggregates| SILVER[(Iceberg Silver<br/>revenue_1m, alerts,<br/>delays_5m, metric_1m)]
  F -->|invalid| DLQ[(Kafka DLQ<br/>orders.dlq)]
  BRONZE & SILVER --> MINIO[(MinIO / S3<br/>Iceberg warehouse)]
  REST[Iceberg REST catalog] --- MINIO
  SILVER --> DBT[dbt Core] --> GOLD[(Iceberg Gold<br/>facts/dims/marts)]
  GOLD --> TRINO[Trino]
  TRINO --> API[FastAPI]
  TRINO --> SUP[Superset]
  API --> METRICS[/Prometheus → Grafana/]
  DAG[Dagster] -.orchestrates.-> DBT
  DAG -.triggers.-> ML[MLflow models]
  GOLD --> ML
  GOLD --> TWIN[Digital twin<br/>SimPy + Monte Carlo]
  DOCS[(SOPs / contracts)] --> RAG[Qdrant + Ollama RAG] --> API
```

## Why these components
- **Kafka (KRaft)** — durable, replayable event bus; auto-creates topics in dev.
- **Flink SQL** — declarative stream processing with event-time windows,
  exactly-once checkpointing to Iceberg, and a SQL-native DLQ split.
- **Iceberg on MinIO + REST catalog** — open table format (ACID, schema
  evolution, time travel) on S3-compatible storage; one warehouse, many engines.
- **Trino** — federated SQL engine that both dbt and the API/Superset share.
- **dbt Core** — versioned, tested Bronze→Silver→Gold transformations.
- **Dagster** — code-defined orchestration of the batch/ML/quality assets.
- **FastAPI / Superset** — programmatic + visual consumption.
- **Prometheus/Grafana** — metrics; the API exposes `/metrics`.

## Medallion layers
| Layer | Owner | Contents |
|---|---|---|
| Bronze | Flink | validated raw events (`bronze.*_raw`) |
| Silver | Flink | streaming aggregates/alerts (`silver.*`) |
| Gold | dbt | business marts: `fct_revenue_hourly`, `agg_inventory_health`, `fct_carrier_performance`, `fct_iot_hourly` |

## Roadmap layers (scaffolded, see per-folder READMEs)
- **ML/AI** (`ml/`) — forecasting, anomaly detection, supplier risk → MLflow.
- **RAG** (`rag/`) — embeddings in Qdrant, answers via local Ollama LLM.
- **Digital twin** (`digital_twin/`) — SimPy + Monte Carlo what-if simulation.
- **Metadata/Knowledge graph** — DataHub (run as its own compose stack and point
  its ingestion recipes at Kafka/Trino/dbt; lineage from dbt `manifest.json`).
- **Governance** (`governance/`) — RBAC, masking, audit, data contracts.
- **Ingestion connectors** — Airbyte OSS (API sources) + Debezium (CDC) feeding
  the same Kafka topics the generator uses today.
