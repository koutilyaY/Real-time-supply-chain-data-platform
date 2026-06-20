#!/usr/bin/env bash
# Back up ALL Postgres databases (Iceberg REST catalog + Dagster/MLflow/Superset
# metadata) to a gzipped pg_dumpall. Keeps the last 7 backups.
#
# Usage: ./scripts/backup_postgres.sh
# Schedule (cron):  30 2 * * *  cd /path/to/repo && ./scripts/backup_postgres.sh
set -euo pipefail
cd "$(dirname "$0")/.."

mkdir -p backups
TS=$(docker compose exec -T postgres date -u +%Y%m%d_%H%M%S | tr -d '\r')
OUT="backups/postgres_${TS}.sql.gz"

echo "Dumping all databases -> ${OUT}"
docker compose exec -T postgres pg_dumpall -U "${POSTGRES_USER:-platform}" | gzip > "${OUT}"
echo "  wrote ${OUT} ($(du -h "${OUT}" | cut -f1))"

# Retention: keep the 7 most recent.
ls -1t backups/postgres_*.sql.gz 2>/dev/null | tail -n +8 | xargs -r rm -f
echo "  retained $(ls -1 backups/postgres_*.sql.gz 2>/dev/null | wc -l | tr -d ' ') backup(s)"
echo "Restore with:  gunzip -c ${OUT} | docker compose exec -T postgres psql -U ${POSTGRES_USER:-platform}"
