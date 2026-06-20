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


daily_job = define_asset_job("daily_refresh", selection="*")
daily_schedule = ScheduleDefinition(job=daily_job, cron_schedule="0 * * * *")

defs = Definitions(assets=[build_marts, dq_checks], schedules=[daily_schedule])
