#!/usr/bin/env bash
# Iceberg table maintenance via Trino:
#   1. optimize            - compact small files (streaming writes one file/checkpoint)
#   2. expire_snapshots    - drop old snapshots (metadata + unreferenced data files)
#   3. remove_orphan_files - delete files no live snapshot references
#
# Usage: ./scripts/iceberg_maintain.sh [retention]   (default 7d; dev demo: 1m)
# Trino enforces a 7d min-retention floor; the job lowers it per-session for the
# retention it was explicitly asked to use (safe — operator-chosen, not global).
set -uo pipefail
cd "$(dirname "$0")/.."

RETENTION="${1:-7d}"
TABLES=(
  bronze.orders_raw bronze.inventory_raw bronze.shipments_raw bronze.iot_raw
  silver.orders_revenue_1m silver.inventory_alerts silver.shipment_delays_5m silver.iot_metric_1m
)

# Returns 0 on success; prints the Trino error line on failure.
run() {
  local out
  out=$(docker compose exec -T trino trino --execute "$1" 2>&1 | grep -ivE "WARNING: Unable|jline")
  if echo "$out" | grep -qi "failed"; then
    echo "      ! $(echo "$out" | grep -i failed | head -1)"
    return 1
  fi
}

echo "Iceberg maintenance (retention=${RETENTION})"
for t in "${TABLES[@]}"; do
  echo "==> iceberg.${t}"
  run "ALTER TABLE iceberg.${t} EXECUTE optimize" && echo "      compacted small files"
  run "SET SESSION iceberg.expire_snapshots_min_retention = '${RETENTION}'; ALTER TABLE iceberg.${t} EXECUTE expire_snapshots(retention_threshold => '${RETENTION}')" \
    && echo "      expired snapshots older than ${RETENTION}"
  run "SET SESSION iceberg.remove_orphan_files_min_retention = '${RETENTION}'; ALTER TABLE iceberg.${t} EXECUTE remove_orphan_files(retention_threshold => '${RETENTION}')" \
    && echo "      removed orphan files older than ${RETENTION}"
done
echo "Maintenance complete."
