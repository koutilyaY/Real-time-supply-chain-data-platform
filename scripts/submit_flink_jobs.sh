#!/usr/bin/env bash
# Submit the four Flink SQL streaming jobs (one detached job per domain).
set -euo pipefail
cd "$(dirname "$0")/.."

SQL=/opt/flink/sql
JOBS=(orders inventory shipments iot)

for j in "${JOBS[@]}"; do
  echo "==> submitting ${j}_pipeline"
  docker compose exec -T flink-jobmanager \
    /opt/flink/bin/sql-client.sh \
      -i "${SQL}/00_init.sql" \
      -f "${SQL}/${j}_pipeline.sql"
done

echo "All jobs submitted. Watch them at http://localhost:8081"
