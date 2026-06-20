"""Thin Trino access layer used by the API."""
import os
from functools import lru_cache

import trino

TRINO_HOST = os.getenv("TRINO_HOST", "localhost")
TRINO_PORT = int(os.getenv("TRINO_PORT", "8080"))
TRINO_USER = os.getenv("TRINO_USER", "api")
TRINO_CATALOG = os.getenv("TRINO_CATALOG", "iceberg")


@lru_cache(maxsize=1)
def _conn_kwargs() -> dict:
    return dict(host=TRINO_HOST, port=TRINO_PORT, user=TRINO_USER, catalog=TRINO_CATALOG)


def query(sql: str, params: tuple | None = None) -> list[dict]:
    """Run a SQL query and return rows as dicts. Raises trino exceptions on error."""
    conn = trino.dbapi.connect(**_conn_kwargs())
    try:
        cur = conn.cursor()
        cur.execute(sql, params or None)
        cols = [d[0] for d in cur.description] if cur.description else []
        return [dict(zip(cols, row)) for row in cur.fetchall()]
    finally:
        conn.close()


def query_safe(sql: str, params: tuple | None = None) -> tuple[list[dict], str | None]:
    """Like query() but never raises; returns (rows, error_message)."""
    try:
        return query(sql, params), None
    except Exception as exc:  # noqa: BLE001 - surface as API note, don't 500 the demo
        return [], str(exc)
