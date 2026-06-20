#!/usr/bin/env bash
# Register (or update) the Debezium Postgres connector against Kafka Connect.
set -euo pipefail
cd "$(dirname "$0")/.."

CONNECT_URL="${CONNECT_URL:-http://localhost:8083}"
CFG="ingestion/cdc/debezium-postgres.json"

echo "Waiting for Kafka Connect at ${CONNECT_URL} ..."
for i in $(seq 1 30); do
  if curl -fsS "${CONNECT_URL}/connectors" >/dev/null 2>&1; then break; fi
  sleep 3
done

echo "Registering connector from ${CFG} ..."
# Idempotent: PUT the config to /connectors/<name>/config
NAME=$(python3 -c "import json;print(json.load(open('${CFG}'))['name'])")
CONFIG=$(python3 -c "import json;print(json.dumps(json.load(open('${CFG}'))['config']))")
curl -fsS -X PUT "${CONNECT_URL}/connectors/${NAME}/config" \
  -H 'Content-Type: application/json' -d "${CONFIG}" | python3 -m json.tool | head -20

echo
echo "Connector status:"
sleep 4
curl -fsS "${CONNECT_URL}/connectors/${NAME}/status" | python3 -m json.tool
