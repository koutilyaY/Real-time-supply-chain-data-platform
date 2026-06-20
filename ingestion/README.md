# Ingestion

In the vertical slice, a **synthetic generator** stands in for the source systems
so the whole platform runs with zero external dependencies. The events it emits
match the **data contracts** in `generator/contracts/*.schema.json`, which are the
same shapes real connectors must honor.

## Synthetic generator
- `generator/generator.py` produces referentially-consistent events for
  `orders`, `inventory`, `shipments`, `suppliers`, `iot.sensors`.
- A configurable fraction (`GEN_BAD_RATE`, default 3%) are intentionally
  malformed to exercise the Flink DLQ path.
- Runs as the `generator` service (`docker compose up`).

## Replacing with real sources (next pass)
The generator publishes to the same Kafka topics real connectors would:

1. **API sources → Airbyte OSS.** Deploy Airbyte (its own compose stack), add
   sources (REST APIs, SaaS), and a **Kafka destination** writing to these topics.
2. **Database CDC → Debezium.** Run Debezium (Kafka Connect) against ERP/WMS/TMS
   Postgres/MySQL with log-based CDC → topics like `erp.public.orders`. Point the
   Flink jobs at those topics (or add a normalization job).
3. **Schemas/contracts.** Promote the JSON Schemas here into a schema registry
   (Apicurio is Apache-2.0) and switch the Kafka `format` to Avro for enforced
   compatibility. Keep the contracts versioned and CI-validated.
