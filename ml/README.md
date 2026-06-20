# ML & AI

Three runnable training scripts read curated Gold marts (via Trino) and log to
**MLflow**. They are intentionally small models — the point is the full loop:
lakehouse → features → train → track → (register) → serve.

## Run
```bash
# 1. start MLflow (artifacts in MinIO, metadata in Postgres)
docker compose --profile ml up -d --build   # UI at http://localhost:5000

# 2. install training deps on host
pip install -r ml/requirements-train.txt

# 3. train (after the stream + dbt have produced Gold data)
export TRINO_HOST=localhost MLFLOW_TRACKING_URI=http://localhost:5000
python ml/forecasting/train_forecast.py        # Holt-Winters per region
python ml/anomaly/train_anomaly.py             # IsolationForest on IoT
python ml/supplier_risk/train_supplier_risk.py # GBC on carrier performance
```

Each script skips gracefully when there isn't enough history yet, so it's safe to
run early. Schedule them from Dagster (`orchestration/`) once data accumulates.

## Notes
- `Dockerfile` here builds the **MLflow tracking server** image only.
- Model registry + batch scoring → publish predictions back to an Iceberg Gold
  table or a Kafka topic for the API to serve.
