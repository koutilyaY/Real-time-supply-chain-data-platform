#!/usr/bin/env bash
# Create the lakehouse namespaces (bronze/silver/gold) via Trino.
# Flink also creates bronze/silver on demand; this makes them explicit and adds gold.
set -euo pipefail
cd "$(dirname "$0")/.."

run() { docker compose exec -T trino trino --execute "$1"; }

echo "Creating Iceberg namespaces..."
run "CREATE SCHEMA IF NOT EXISTS iceberg.bronze"
run "CREATE SCHEMA IF NOT EXISTS iceberg.silver"
run "CREATE SCHEMA IF NOT EXISTS iceberg.gold"
echo "Namespaces ready:"
run "SHOW SCHEMAS FROM iceberg"
