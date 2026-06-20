"""Shared helpers for the ML training scripts."""
import os

import pandas as pd
import trino

TRINO_HOST = os.getenv("TRINO_HOST", "localhost")
TRINO_PORT = int(os.getenv("TRINO_PORT", "8080"))
MLFLOW_URI = os.getenv("MLFLOW_TRACKING_URI", "http://localhost:5000")


def read_sql(sql: str) -> pd.DataFrame:
    conn = trino.dbapi.connect(host=TRINO_HOST, port=TRINO_PORT, user="ml", catalog="iceberg")
    try:
        cur = conn.cursor()
        cur.execute(sql)
        cols = [d[0] for d in cur.description]
        return pd.DataFrame(cur.fetchall(), columns=cols)
    finally:
        conn.close()
