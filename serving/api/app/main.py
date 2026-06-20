"""
Supply-Chain Platform API (FastAPI over Trino/Iceberg).

Read endpoints serve curated Gold/Silver tables produced by Flink + dbt.
All query endpoints degrade gracefully: if a table doesn't exist yet (e.g. the
streaming jobs haven't produced data), they return an empty result plus a `note`
rather than a 500, so the API is usable the moment the stack is up.
"""
import os
import time

from fastapi import FastAPI, Query, Request
from fastapi.responses import JSONResponse, PlainTextResponse
from prometheus_client import CONTENT_TYPE_LATEST, Counter, Histogram, generate_latest
from slowapi import Limiter
from slowapi.errors import RateLimitExceeded
from slowapi.middleware import SlowAPIMiddleware
from slowapi.util import get_remote_address

from .db import query_safe

API_KEY = os.getenv("API_KEY", "dev-secret-key")
RATE_LIMIT = os.getenv("API_RATE_LIMIT", "120/minute")
# Paths reachable without an API key (probes, scraping, docs).
OPEN_PATHS = {"/health", "/metrics", "/docs", "/openapi.json", "/redoc"}

app = FastAPI(title="Real-Time Supply-Chain Platform API", version="0.1.0")

# --- Rate limiting (per client IP) ---
limiter = Limiter(key_func=get_remote_address, default_limits=[RATE_LIMIT])
app.state.limiter = limiter
app.add_exception_handler(
    RateLimitExceeded,
    lambda request, exc: JSONResponse(status_code=429, content={"detail": "rate limit exceeded"}),
)
app.add_middleware(SlowAPIMiddleware)


# --- API-key auth (everything except OPEN_PATHS) ---
@app.middleware("http")
async def require_api_key(request: Request, call_next):
    if request.url.path not in OPEN_PATHS and request.headers.get("x-api-key") != API_KEY:
        return JSONResponse(status_code=401, content={"detail": "missing or invalid X-API-Key"})
    return await call_next(request)


REQ = Counter("api_requests_total", "API requests", ["endpoint", "status"])
LAT = Histogram("api_request_seconds", "API latency", ["endpoint"])


def served(endpoint: str, sql: str, params: tuple | None = None):
    start = time.perf_counter()
    rows, err = query_safe(sql, params)
    LAT.labels(endpoint).observe(time.perf_counter() - start)
    REQ.labels(endpoint, "error" if err else "ok").inc()
    body = {"count": len(rows), "data": rows}
    if err:
        body["note"] = "query failed (table may not exist yet)"
        body["error"] = err
    return body


@app.get("/health")
def health():
    rows, err = query_safe("SELECT 1 AS ok")
    return {"status": "ok" if not err else "degraded", "trino": not err, "error": err}


@app.get("/metrics")
def metrics():
    return PlainTextResponse(generate_latest(), media_type=CONTENT_TYPE_LATEST)


@app.get("/revenue/hourly")
def revenue_hourly(region: str | None = Query(None), limit: int = 200):
    where = "WHERE region = %s" if region else ""
    params = (region,) if region else None
    sql = f"""
        SELECT region, revenue_hour, orders, gross_revenue
        FROM iceberg.gold.fct_revenue_hourly
        {where}
        ORDER BY revenue_hour DESC
        LIMIT {int(limit)}
    """
    return served("revenue_hourly", sql, params)


@app.get("/inventory/health")
def inventory_health(region: str | None = Query(None)):
    where = "WHERE region = %s" if region else ""
    params = (region,) if region else None
    sql = f"""
        SELECT warehouse_id, region, skus_below_reorder, total_shortfall_units
        FROM iceberg.gold.agg_inventory_health
        {where}
        ORDER BY skus_below_reorder DESC
    """
    return served("inventory_health", sql, params)


@app.get("/inventory/alerts")
def inventory_alerts(limit: int = 100):
    sql = f"""
        SELECT warehouse_id, sku, on_hand, reorder_point, shortfall, region, detected_ts
        FROM iceberg.silver.inventory_alerts
        ORDER BY detected_ts DESC
        LIMIT {int(limit)}
    """
    return served("inventory_alerts", sql)


@app.get("/carriers")
def carriers():
    sql = """
        SELECT carrier, total_shipments, delayed_shipments, delay_rate, on_time_rate
        FROM iceberg.gold.fct_carrier_performance
        ORDER BY on_time_rate ASC
    """
    return served("carriers", sql)


@app.get("/shipments/delays")
def shipment_delays(carrier: str | None = Query(None), limit: int = 100):
    where = "WHERE carrier = %s" if carrier else ""
    params = (carrier,) if carrier else None
    sql = f"""
        SELECT carrier, window_start, window_end, total_shipments,
               delayed_shipments, delay_rate
        FROM iceberg.silver.shipment_delays_5m
        {where}
        ORDER BY window_start DESC
        LIMIT {int(limit)}
    """
    return served("shipment_delays", sql, params)


@app.get("/iot/metrics")
def iot_metrics(metric: str | None = Query(None), limit: int = 200):
    where = "WHERE metric = %s" if metric else ""
    params = (metric,) if metric else None
    sql = f"""
        SELECT warehouse_id, metric, reading_hour, readings, avg_value, min_value, max_value
        FROM iceberg.gold.fct_iot_hourly
        {where}
        ORDER BY reading_hour DESC
        LIMIT {int(limit)}
    """
    return served("iot_metrics", sql, params)


@app.get("/chat")
def chat(q: str = Query(..., description="Natural-language question")):
    """RAG endpoint placeholder. The full retrieval pipeline lives in /rag and
    is enabled with `docker compose --profile rag up`. See rag/README.md."""
    return JSONResponse(
        status_code=501,
        content={
            "answer": None,
            "note": "RAG layer not wired in the vertical slice. "
                    "Start it with `docker compose --profile rag up` and see rag/README.md.",
            "question": q,
        },
    )
