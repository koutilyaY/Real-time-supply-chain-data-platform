#!/usr/bin/env bash
# Evolve Iceberg partition specs to day-partition each table on its event-time
# column (query pruning + smaller manifests). Iceberg partition evolution is
# metadata-only and applies to NEW data; existing files keep their layout.
# Flink picks up the new spec on its next job (re)start.
set -uo pipefail
cd "$(dirname "$0")/.."

run() { docker compose exec -T trino trino --execute "$1" 2>&1 | grep -ivE "WARNING: Unable|jline"; }

declare -a SPECS=(
  "bronze.orders_raw|day(order_ts)"
  "bronze.inventory_raw|day(inventory_ts)"
  "bronze.shipments_raw|day(ship_ts)"
  "bronze.iot_raw|day(reading_ts)"
  "silver.orders_revenue_1m|day(window_start)"
  "silver.inventory_alerts|day(detected_ts)"
  "silver.shipment_delays_5m|day(window_start)"
  "silver.iot_metric_1m|day(window_start)"
)

echo "Applying Iceberg day-partitioning..."
for spec in "${SPECS[@]}"; do
  tbl="${spec%%|*}"; part="${spec##*|}"
  if run "ALTER TABLE iceberg.${tbl} SET PROPERTIES partitioning = ARRAY['${part}']" 2>&1 | grep -qi failed; then
    echo "  ! ${tbl}: failed"
  else
    echo "  ${tbl} -> ${part}"
  fi
done
echo "Done. Restart Flink jobs so new writes honor the spec."
