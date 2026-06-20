#!/usr/bin/env bash
set -e
echo "[superset] migrating metadata db..."
superset db upgrade
echo "[superset] creating admin (idempotent)..."
superset fab create-admin \
  --username "${SUPERSET_ADMIN_USER:-admin}" \
  --firstname Admin --lastname User \
  --email admin@example.com \
  --password "${SUPERSET_ADMIN_PASSWORD:-admin}" || true
superset init
echo "[superset] registering Trino database (idempotent)..."
superset set-database-uri -d Lakehouse -u "trino://api@trino:8080/iceberg" || true
echo "[superset] starting webserver on :8088"
exec gunicorn --bind 0.0.0.0:8088 --workers 2 --timeout 120 "superset.app:create_app()"
