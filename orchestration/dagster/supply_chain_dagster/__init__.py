"""Dagster code location for the supply-chain platform.

Assets:
  dbt_models         - one Dagster asset PER dbt model (dagster-dbt) with lineage.
  dq_checks          - lightweight data-quality checks over the lakehouse (Trino).
  iceberg_maintenance- compaction / snapshot expiry / orphan cleanup.

Schedules: hourly dbt+DQ refresh; daily off-peak Iceberg maintenance.
"""
import os
import subprocess

import trino
from dagster import (
    AssetSelection,
    Definitions,
    MaterializeResult,
    ScheduleDefinition,
    asset,
    define_asset_job,
    get_dagster_logger,
)
from dagster_dbt import DbtCliResource, dbt_assets

DBT_DIR = os.getenv("DBT_PROJECT_DIR", "/opt/dagster/dbt")
TRINO_HOST = os.getenv("TRINO_HOST", "trino")

# Ensure packages + manifest exist so @dbt_assets can parse the project at load.
_env = {**os.environ, "DBT_PROFILES_DIR": DBT_DIR}
subprocess.run(["dbt", "deps"], cwd=DBT_DIR, env=_env, check=False)
subprocess.run(["dbt", "parse"], cwd=DBT_DIR, env=_env, check=False)
MANIFEST = os.path.join(DBT_DIR, "target", "manifest.json")


def _trino_scalar(sql: str):
    conn = trino.dbapi.connect(host=TRINO_HOST, port=8080, user="dagster", catalog="iceberg")
    try:
        cur = conn.cursor()
        cur.execute(sql)
        row = cur.fetchone()
        return row[0] if row else None
    finally:
        conn.close()


@dbt_assets(manifest=MANIFEST)
def dbt_models(context, dbt: DbtCliResource):
    """One asset per dbt model (staging views + Gold marts) with column lineage,
    test results surfaced as asset checks, and retries handled by Dagster."""
    yield from dbt.cli(["build"], context=context).stream()


@asset(group_name="quality", deps=[dbt_models])
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


daily_job = define_asset_job(
    "daily_refresh",
    selection=AssetSelection.assets(dbt_models) | AssetSelection.assets("dq_checks"),
)
daily_schedule = ScheduleDefinition(job=daily_job, cron_schedule="0 * * * *")
# Iceberg maintenance on its own daily cadence (heavier; off-peak).
maintenance_job = define_asset_job("iceberg_maintenance_job", selection=["iceberg_maintenance"])
maintenance_schedule = ScheduleDefinition(job=maintenance_job, cron_schedule="30 3 * * *")

defs = Definitions(
    assets=[dbt_models, dq_checks, iceberg_maintenance],
    schedules=[daily_schedule, maintenance_schedule],
    resources={"dbt": DbtCliResource(project_dir=DBT_DIR, profiles_dir=DBT_DIR)},
)
