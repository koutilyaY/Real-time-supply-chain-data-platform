# Runbook

## Bring it up
```bash
make up          # core slice (build images first time ~ several minutes)
make lake-init   # create iceberg.{bronze,silver,gold} schemas
make flink-jobs  # submit the 4 streaming jobs
# wait ~1-2 min for windows to close, then:
make dbt-run     # build Gold marts (needs `pip install dbt-trino`)
make smoke       # verify end-to-end
```
Or `make demo` for up → lake-init → flink-jobs in one shot.

## Service URLs
| Service | URL | Creds |
|---|---|---|
| MinIO console | http://localhost:9001 | admin / password |
| Kafka (host) | localhost:29092 | — |
| Flink UI | http://localhost:8081 | — |
| Iceberg REST | http://localhost:8181 | — |
| Trino | http://localhost:8080 | user any |
| API (docs) | http://localhost:8000/docs | — |
| Superset | http://localhost:8088 | admin / admin |
| Dagster | http://localhost:3000 | — |
| MLflow | http://localhost:5000 | — |
| Qdrant | http://localhost:6333 | — |
| Grafana | http://localhost:3001 | anon / admin |
| Prometheus | http://localhost:9090 | — |

## Common checks
```bash
# topics
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 --list
# peek at events
docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server kafka:9092 --topic orders --max-messages 3
# DLQ depth
docker compose exec kafka /opt/kafka/bin/kafka-run-class.sh kafka.tools.GetOffsetShell \
  --broker-list kafka:9092 --topic orders.dlq
# query the lakehouse
docker compose exec trino trino --execute "SELECT * FROM iceberg.silver.orders_revenue_1m LIMIT 10"
```

## Troubleshooting
- **Flink job fails on S3/Iceberg auth** — confirm `AWS_ACCESS_KEY_ID/SECRET` are
  set (compose injects them) and `iceberg-rest` is healthy.
- **Trino `SCHEMA does not exist`** — run `make lake-init`, or let Flink jobs run
  once (they create bronze/silver).
- **dbt can't connect** — Trino must be up on :8080; set `TRINO_HOST` if remote.
- **No Gold data** — Silver windows must close first (≥1 min of stream), then
  `make dbt-run`.
- **Superset slow first boot** — it runs migrations on first start; give it a
  minute.

## Chaos
`make chaos` kills taskmanager → minio → kafka in turn and re-runs the smoke
test, confirming Flink resumes from checkpoints and the DLQ keeps capturing.
```
