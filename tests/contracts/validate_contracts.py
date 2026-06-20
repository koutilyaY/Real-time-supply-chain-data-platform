"""
Data-contract enforcement test.

Two guarantees, checked against the JSON Schemas in
ingestion/generator/contracts/:

  1. CONFORMANCE  - every well-formed event the generator produces validates
                    against its contract (no silent drift between producer & schema).
  2. REJECTION    - known-malformed events are rejected by the contract
                    (the same bad shapes the generator injects for the DLQ).

Run:  python tests/contracts/validate_contracts.py
Exit 0 = all guarantees hold.
"""
import json
import os
import sys

from jsonschema import Draft202012Validator, FormatChecker

ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
CONTRACTS = os.path.join(ROOT, "ingestion", "generator", "contracts")
sys.path.insert(0, os.path.join(ROOT, "ingestion", "generator"))

# Force clean events for the conformance pass.
os.environ["GEN_BAD_RATE"] = "0"
import generator as g  # noqa: E402

# generator function -> contract file
CASES = [
    (g.gen_order, "orders.schema.json"),
    (g.gen_inventory, "inventory.schema.json"),
    (g.gen_shipment, "shipments.schema.json"),
    (g.gen_supplier, "suppliers.schema.json"),
    (g.gen_iot, "iot_sensors.schema.json"),
]

# Explicit malformed samples that MUST be rejected (mirror the DLQ injection modes).
BAD_SAMPLES = {
    "orders.schema.json": [
        {"event_id": "e", "order_id": "O1", "customer_id": "C1", "sku": None,
         "quantity": 5, "unit_price": 1.0, "order_ts": "2026-01-01T00:00:00.000Z"},   # null sku
        {"event_id": "e", "order_id": "O1", "customer_id": "C1", "sku": "SKU-00001",
         "quantity": -3, "unit_price": 1.0, "order_ts": "2026-01-01T00:00:00.000Z"},  # neg qty
        {"event_id": "e", "customer_id": "C1", "sku": "SKU-00001",
         "quantity": 5, "unit_price": 1.0, "order_ts": "2026-01-01T00:00:00.000Z"},   # missing order_id
        {"event_id": "e", "order_id": "O1", "customer_id": "C1", "sku": "SKU-00001",
         "quantity": 5, "unit_price": 1.0, "order_ts": "not-a-timestamp"},            # bad ts
    ],
    "inventory.schema.json": [
        {"event_id": "e", "warehouse_id": "WH-01", "sku": "SKU-00001",
         "on_hand": -1, "inventory_ts": "2026-01-01T00:00:00.000Z"},                  # neg on_hand
    ],
    "iot_sensors.schema.json": [
        {"event_id": "e", "sensor_id": "S1", "metric": "pressure", "value": 1.0,
         "reading_ts": "2026-01-01T00:00:00.000Z"},                                   # bad enum
    ],
}


def validator(schema_file):
    with open(os.path.join(CONTRACTS, schema_file)) as fh:
        return Draft202012Validator(json.load(fh), format_checker=FormatChecker())


def main() -> int:
    failures = []

    # 1. Conformance: many clean events per type must all validate.
    for gen_fn, schema_file in CASES:
        v = validator(schema_file)
        bad = 0
        for _ in range(2000):
            _, _, event = gen_fn()
            errs = sorted(v.iter_errors(event), key=str)
            if errs:
                bad += 1
                if bad <= 2:
                    failures.append(f"CONFORMANCE {schema_file}: {errs[0].message} :: {event}")
        mark = "PASS" if bad == 0 else "FAIL"
        print(f"[{mark}] conformance {schema_file:28s} 2000 events, {bad} invalid")

    # 2. Rejection: each malformed sample must be rejected.
    for schema_file, samples in BAD_SAMPLES.items():
        v = validator(schema_file)
        for i, sample in enumerate(samples):
            rejected = bool(list(v.iter_errors(sample)))
            mark = "PASS" if rejected else "FAIL"
            print(f"[{mark}] rejection   {schema_file:28s} bad sample #{i} rejected={rejected}")
            if not rejected:
                failures.append(f"REJECTION {schema_file}: bad sample #{i} was accepted: {sample}")

    print()
    if failures:
        print(f"CONTRACTS FAILED ({len(failures)}):")
        for f in failures[:10]:
            print("  -", f)
        return 1
    print("CONTRACTS OK")
    return 0


if __name__ == "__main__":
    sys.exit(main())
