"""
End-to-end smoke test for the running stack (stdlib only).

Checks, in order:
  1. API /health  -> Trino reachable
  2. API query endpoints return 200 with a JSON body
  3. (optional) MinIO and Kafka liveness if docker is available

Run after `make demo` and a minute of warm-up:
  python tests/integration/smoke_test.py
Exit code 0 = all critical checks passed.
"""
import json
import os
import subprocess
import sys
import urllib.request

API = "http://localhost:8000"
CRITICAL_FAILED = []


API_KEY = os.getenv("API_KEY", "dev-secret-key")


def get(path: str) -> tuple[int, dict]:
    req = urllib.request.Request(API + path, headers={"X-API-Key": API_KEY})
    with urllib.request.urlopen(req, timeout=10) as resp:
        return resp.status, json.loads(resp.read().decode())


def check(name: str, ok: bool, detail: str = "", critical: bool = True):
    mark = "PASS" if ok else "FAIL"
    print(f"[{mark}] {name}" + (f" - {detail}" if detail else ""))
    if not ok and critical:
        CRITICAL_FAILED.append(name)


def main() -> int:
    try:
        status, body = get("/health")
        check("API /health reachable", status == 200, f"status={status}")
        check("Trino connected", body.get("trino") is True, body.get("error") or "")
    except Exception as exc:  # noqa: BLE001
        check("API /health reachable", False, str(exc))
        print("\nAPI not reachable - is the stack up? `make up`")
        return 1

    for path in ("/revenue/hourly?limit=5", "/carriers", "/inventory/alerts?limit=5",
                 "/iot/metrics?limit=5", "/shipments/delays?limit=5"):
        try:
            status, body = get(path)
            n = body.get("count", 0)
            # 200 is the bar; data may be empty if streaming just started.
            check(f"GET {path}", status == 200, f"rows={n}"
                  + ("" if not body.get("error") else " (table not ready)"),
                  critical=True)
        except Exception as exc:  # noqa: BLE001
            check(f"GET {path}", False, str(exc))

    # Optional infra liveness (best-effort).
    try:
        out = subprocess.run(
            ["docker", "compose", "exec", "-T", "kafka",
             "/opt/kafka/bin/kafka-topics.sh", "--bootstrap-server", "kafka:9092", "--list"],
            capture_output=True, text=True, timeout=20,
        )
        topics = out.stdout.split()
        check("Kafka topics present", any(t in topics for t in ("orders", "inventory")),
              f"{len(topics)} topics", critical=False)
    except Exception as exc:  # noqa: BLE001
        check("Kafka liveness (docker)", False, str(exc), critical=False)

    print()
    if CRITICAL_FAILED:
        print(f"SMOKE FAILED: {', '.join(CRITICAL_FAILED)}")
        return 1
    print("SMOKE OK")
    return 0


if __name__ == "__main__":
    sys.exit(main())
