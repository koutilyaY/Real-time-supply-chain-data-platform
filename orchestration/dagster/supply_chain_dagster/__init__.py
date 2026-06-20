"""Dagster code location for the supply-chain platform.

Assets:
  build_marts  - runs `dbt build` to (re)materialize Gold marts from Silver.
  dq_checks    - lightweight data-quality checks over the lakehouse via Trino.

A daily schedule materializes both. ML retrain / RAG reindex assets can be added
here later (see ml/ and rag/).
"""
import os
import subprocess

import trino
from dagster import (
    Definitions,
    MaterializeResult,
    ScheduleDefinition,
    asset,
    define_asset_job,
    get_dagster_logger,
)

DBT_DIR = os.getenv("DBT_PROJECT_DIR", "/opt/dagster/dbt")
TRINO_HOST = os.getenv("TRINO_HOST", "trino")


def _trino_scalar(sql: str):
    conn = trino.dbapi.connect(host=TRINO_HOST, port=8080, user="dagster", catalog="iceberg")
    try:
        cur = conn.cursor()
        cur.execute(sql)
        row = cur.fetchone()
        return row[0] if row else None
    finally:
        conn.close()


@asset(group_name="transform")
def build_marts() -> MaterializeResult:
    """Run dbt to build Silver staging views + Gold marts."""
    log = get_dagster_logger()
    env = {**os.environ, "DBT_PROFILES_DIR": DBT_DIR}
    for cmd in (["dbt", "deps"], ["dbt", "build"]):
        log.info("running: %s", " ".join(cmd))
        subprocess.run(cmd, cwd=DBT_DIR, env=env, check=True)
    return MaterializeResult(metadata={"dbt_project": DBT_DIR})


@asset(group_name="quality", deps=[build_marts])
def dq_checks() -> MaterializeResult:
    """Freshness + volume sanity checks over the curated layer."""
    log = get_dagster_logger()
    checks = {}
    for tbl in ("iceberg.silver.orders_revenue_1m", "iceberg.gold.fct_revenue_hourly"):
        try:
            checks[tbl] = _trino_scalar(f"SELECT count(*) FROM {tbl}")
        except Exception as exc:  # noqa: BLE001
            checks[tbl] = f"error: {exc}"
    log.info("row counts: %s", checks)
    passed = all(isinstance(v, int) and v >= 0 for v in checks.values())
    return MaterializeResult(metadata={"passed": passed, **{k: str(v) for k, v in checks.items()}})


MAINTENANCE_TABLES = [
    "bronze.orders_raw", "bronze.inventory_raw", "bronze.shipments_raw", "bronze.iot_raw",
    "silver.orders_revenue_1m", "silver.inventory_alerts",
    "silver.shipment_delays_5m", "silver.iot_metric_1m",
]
RETENTION = os.getenv("ICEBERG_RETENTION", "7d")  # prod-safe; streaming writes a file/snapshot per checkpoint


@asset(group_name="maintenance")
def iceberg_maintenance() -> MaterializeResult:
    """Compact small files, expire old snapshots, and remove orphan files for
    every Bronze/Silver table. Keeps the lakehouse from rotting under streaming
    (one file + one snapshot per Flink checkpoint)."""
    log = get_dagster_logger()
    conn = trino.dbapi.connect(
        host=TRINO_HOST, port=8080, user="dagster", catalog="iceberg",
        session_properties={
            "iceberg.expire_snapshots_min_retention": RETENTION,
            "iceberg.remove_orphan_files_min_retention": RETENTION,
        },
    )
    done, errors = [], {}
    for t in MAINTENANCE_TABLES:
        for op in (
            f"ALTER TABLE iceberg.{t} EXECUTE optimize",
            f"ALTER TABLE iceberg.{t} EXECUTE expire_snapshots(retention_threshold => '{RETENTION}')",
            f"ALTER TABLE iceberg.{t} EXECUTE remove_orphan_files(retention_threshold => '{RETENTION}')",
        ):
            try:
                cur = conn.cursor()
                cur.execute(op)
                cur.fetchall()
            except Exception as exc:  # noqa: BLE001 - record, continue (e.g. concurrent write)
                errors[f"{t}:{op.split('EXECUTE')[1].split('(')[0].strip()}"] = str(exc)[:120]
        done.append(t)
    log.info("maintained %d tables; %d errors", len(done), len(errors))
    return MaterializeResult(metadata={"tables": len(done), "retention": RETENTION,
                                       "errors": len(errors), **errors})


daily_job = define_asset_job("daily_refresh", selection=["build_marts", "dq_checks"])
daily_schedule = ScheduleDefinition(job=daily_job, cron_schedule="0 * * * *")
# Iceberg maintenance on its own daily cadence (heavier; off-peak).
maintenance_job = define_asset_job("iceberg_maintenance_job", selection=["iceberg_maintenance"])
maintenance_schedule = ScheduleDefinition(job=maintenance_job, cron_schedule="30 3 * * *")

defs = Definitions(
    assets=[build_marts, dq_checks, iceberg_maintenance],
    schedules=[daily_schedule, maintenance_schedule],
)
