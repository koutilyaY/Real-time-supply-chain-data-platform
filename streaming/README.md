# Streaming

The Flink SQL streaming jobs live in **`infra/flink/sql/`** (co-located with the
custom Flink image that bundles the Iceberg + Kafka connectors):

| File | Job |
|---|---|
| `00_init.sql` | creates the Iceberg REST catalog + `bronze`/`silver` namespaces (loaded via `sql-client -i`) |
| `orders_pipeline.sql` | validate → Bronze + 1-min revenue (Silver) + DLQ |
| `inventory_pipeline.sql` | Bronze + low-stock alerts (Silver) |
| `shipments_pipeline.sql` | Bronze + 5-min delay rate per carrier (Silver) |
| `iot_pipeline.sql` | Bronze + 1-min metric aggregates (Silver) |

Submit them with `make flink-jobs` (wraps `scripts/submit_flink_jobs.sh`), watch
at http://localhost:8081.

Each job uses event-time tumbling windows with watermarks, exactly-once
checkpointing to Iceberg (10s interval, RocksDB state), and a SQL-native
dead-letter split for malformed records.
