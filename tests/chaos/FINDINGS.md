# Chaos Validation Findings

Run: `make chaos` (kills flink-taskmanager → minio → kafka, each then restarted,
followed by the integration smoke test). Result below from the 2026-06-19 run.

## ✅ What recovered automatically
| Failure injected | Outcome |
|---|---|
| Kill **flink-taskmanager** | Jobs resumed from the last checkpoint; no data loss. |
| Kill **minio** (object store) | Iceberg writes retried; REST catalog + Trino reconnected. |
| Kill **kafka** (broker) | Broker restarted (data preserved via the image's `/var/lib/kafka/data` volume); Flink jobs reconnected and resumed. |
| Post-chaos **smoke test** | All 8 checks PASS — API served Gold data throughout (durable lakehouse). |
| **DLQ** (`orders.dlq`) | Malformed records still captured (`null_order_id`, `bad_quantity`). |

The durable layers (Iceberg/MinIO, Trino, API) and the Flink jobs are resilient.

## ⚠️ Gap found → fixed
**The synthetic generator did not self-heal after a Kafka broker restart.** Its
confluent-kafka producer runs with `enable.idempotence=true`; when the broker is
killed mid-flight the producer can enter a **fatal** state, the process exits, and
with no restart policy the container stayed `Exited (1)` — stalling the stream
(the durable data was fine; the *source* stopped).

**Fix:** added `restart: unless-stopped` to the generator and the core
long-running services (kafka, minio, iceberg-rest, trino, api, flink-jobmanager,
flink-taskmanager, postgres). Docker now restarts a crashed producer
automatically; verified the stream resumes (+rows after recreate).

## Follow-ups (not blocking)
- Kafka currently uses the image's anonymous `/var/lib/kafka/data` volume — it
  survives `kill`/`restart` but not a `down`/recreate. For stronger durability,
  bind it to a named volume.
- Consider making the generator catch fatal producer errors and rebuild the
  producer in-process (belt-and-suspenders alongside the restart policy).
- Add Debezium connector liveness re-check after a Kafka outage (its internal
  `_connect_*` topics live in the same broker).
