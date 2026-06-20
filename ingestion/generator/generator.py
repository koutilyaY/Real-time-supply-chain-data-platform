"""
Synthetic supply-chain event generator.

Emits realistic, referentially-consistent events into Kafka topics that stand in
for ERP/WMS/TMS/CRM/IoT sources. A small fixed set of dimensions (suppliers,
warehouses, SKUs, customers) is generated up front; transactional events then
reference those keys so downstream joins are meaningful.

A configurable fraction of events are intentionally malformed (negative qty,
null sku, bad timestamps) to exercise the Flink dead-letter-queue path.

Topics (one record = one JSON value, key = business id):
  orders, inventory, shipments, suppliers, iot.sensors

Env:
  KAFKA_BOOTSTRAP     default kafka:9092
  GEN_EVENTS_PER_SEC  default 20   (total across all topics)
  GEN_SEED            default 42
  GEN_BAD_RATE        default 0.03 (fraction of malformed records)
"""
import json
import os
import random
import signal
import sys
import time
from datetime import datetime, timezone

from confluent_kafka import Producer

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
EPS = float(os.getenv("GEN_EVENTS_PER_SEC", "20"))
SEED = int(os.getenv("GEN_SEED", "42"))
BAD_RATE = float(os.getenv("GEN_BAD_RATE", "0.03"))

random.seed(SEED)

REGIONS = ["NA", "EU", "APAC", "LATAM"]
CARRIERS = ["DHL", "FedEx", "UPS", "Maersk", "DBSchenker"]
CURRENCIES = ["USD", "EUR", "GBP"]
COUNTRIES = ["US", "DE", "CN", "MX", "IN", "BR", "NL"]
METRICS = [("temperature", "C"), ("vibration", "mm/s"), ("humidity", "%")]

# -- Dimensions ---------------------------------------------------------------
SUPPLIERS = [
    {
        "supplier_id": f"SUP-{i:03d}",
        "name": f"Supplier {i}",
        "country": random.choice(COUNTRIES),
        "lead_time_days": random.randint(3, 45),
        "on_time_rate": round(random.uniform(0.70, 0.99), 3),
    }
    for i in range(1, 26)
]
WAREHOUSES = [
    {"warehouse_id": f"WH-{i:02d}", "region": random.choice(REGIONS)}
    for i in range(1, 9)
]
SKUS = [
    {
        "sku": f"SKU-{i:05d}",
        "supplier_id": random.choice(SUPPLIERS)["supplier_id"],
        "unit_price": round(random.uniform(2.0, 500.0), 2),
    }
    for i in range(1, 201)
]
CUSTOMERS = [f"CUST-{i:05d}" for i in range(1, 501)]
ASSETS = [f"ASSET-{i:03d}" for i in range(1, 41)]


def now_iso() -> str:
    # millisecond precision + 'Z' suffix -> parses cleanly with Flink TO_TIMESTAMP
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def maybe_corrupt(rec: dict, key_fields: list[str]) -> dict:
    """Randomly damage a record to exercise downstream validation/DLQ."""
    if random.random() >= BAD_RATE:
        return rec
    mode = random.choice(["null_key", "neg_qty", "bad_ts", "drop_field"])
    if mode == "null_key" and key_fields:
        rec[random.choice(key_fields)] = None
    elif mode == "neg_qty":
        for f in ("quantity", "on_hand", "value"):
            if f in rec:
                rec[f] = -abs(rec[f]) - 1
    elif mode == "bad_ts":
        for f in list(rec):
            if f.endswith("_ts"):
                rec[f] = "not-a-timestamp"
    elif mode == "drop_field" and key_fields:
        rec.pop(random.choice(key_fields), None)
    rec["_corrupt"] = mode
    return rec


def gen_order() -> tuple[str, str, dict]:
    sku = random.choice(SKUS)
    rec = {
        "event_id": f"evt-{random.getrandbits(48):x}",
        "order_id": f"ORD-{random.getrandbits(40):x}",
        "customer_id": random.choice(CUSTOMERS),
        "sku": sku["sku"],
        "quantity": random.randint(1, 50),
        "unit_price": sku["unit_price"],
        "currency": random.choice(CURRENCIES),
        "status": random.choice(["created", "picked", "packed", "shipped"]),
        "region": random.choice(REGIONS),
        "order_ts": now_iso(),
    }
    return "orders", rec["order_id"], maybe_corrupt(rec, ["sku", "order_id"])


def gen_inventory() -> tuple[str, str, dict]:
    sku = random.choice(SKUS)
    wh = random.choice(WAREHOUSES)
    on_hand = random.randint(0, 1000)
    rec = {
        "event_id": f"evt-{random.getrandbits(48):x}",
        "warehouse_id": wh["warehouse_id"],
        "sku": sku["sku"],
        "on_hand": on_hand,
        "reserved": random.randint(0, min(on_hand, 200)),
        "reorder_point": random.randint(20, 150),
        "region": wh["region"],
        "inventory_ts": now_iso(),
    }
    return "inventory", f'{rec["warehouse_id"]}|{rec["sku"]}', maybe_corrupt(rec, ["sku"])


def gen_shipment() -> tuple[str, str, dict]:
    rec = {
        "event_id": f"evt-{random.getrandbits(48):x}",
        "shipment_id": f"SHP-{random.getrandbits(40):x}",
        "order_id": f"ORD-{random.getrandbits(40):x}",
        "carrier": random.choice(CARRIERS),
        "origin": random.choice(WAREHOUSES)["warehouse_id"],
        "destination": random.choice(REGIONS),
        "status": random.choice(["in_transit", "delivered", "delayed", "customs_hold"]),
        "ship_ts": now_iso(),
        "eta_ts": now_iso(),
    }
    return "shipments", rec["shipment_id"], maybe_corrupt(rec, ["shipment_id"])


def gen_supplier() -> tuple[str, str, dict]:
    s = dict(random.choice(SUPPLIERS))
    # simulate drift in performance
    s["on_time_rate"] = round(min(0.999, max(0.5, s["on_time_rate"] + random.uniform(-0.05, 0.05))), 3)
    s["risk_score"] = round((1 - s["on_time_rate"]) * 100 + random.uniform(0, 10), 2)
    s["event_id"] = f"evt-{random.getrandbits(48):x}"
    s["updated_ts"] = now_iso()
    return "suppliers", s["supplier_id"], s


def gen_iot() -> tuple[str, str, dict]:
    metric, unit = random.choice(METRICS)
    base = {"temperature": 4.0, "vibration": 2.0, "humidity": 45.0}[metric]
    rec = {
        "event_id": f"evt-{random.getrandbits(48):x}",
        "sensor_id": f"SENS-{random.randint(1, 120):04d}",
        "asset_id": random.choice(ASSETS),
        "warehouse_id": random.choice(WAREHOUSES)["warehouse_id"],
        "metric": metric,
        "value": round(base + random.gauss(0, base * 0.25), 3),
        "unit": unit,
        "reading_ts": now_iso(),
    }
    return "iot.sensors", rec["sensor_id"], maybe_corrupt(rec, ["value"])


# weighted mix of event kinds
GENERATORS = (
    [gen_order] * 8
    + [gen_inventory] * 5
    + [gen_shipment] * 4
    + [gen_iot] * 6
    + [gen_supplier] * 1
)

_running = True


def _stop(*_):
    global _running
    _running = False


def main() -> int:
    signal.signal(signal.SIGTERM, _stop)
    signal.signal(signal.SIGINT, _stop)

    producer = Producer(
        {
            "bootstrap.servers": BOOTSTRAP,
            "linger.ms": 50,
            "compression.type": "lz4",
            "enable.idempotence": True,
        }
    )

    interval = 1.0 / EPS if EPS > 0 else 0.05
    n = 0
    print(f"[generator] -> {BOOTSTRAP} at {EPS} evt/s (bad_rate={BAD_RATE})", flush=True)
    while _running:
        topic, key, value = random.choice(GENERATORS)()
        producer.produce(topic, key=str(key).encode(), value=json.dumps(value).encode())
        n += 1
        if n % 200 == 0:
            producer.poll(0)
            print(f"[generator] produced {n} events", flush=True)
        time.sleep(interval)
    producer.flush(10)
    print(f"[generator] stopped after {n} events", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
