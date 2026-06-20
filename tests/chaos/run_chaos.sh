#!/usr/bin/env bash
# Chaos experiments: kill core services one at a time and verify the platform
# recovers. Kafka retains data on disk; Flink restarts from checkpoints; the DLQ
# captures malformed records throughout. Run against a warmed-up `make demo`.
set -uo pipefail
cd "$(dirname "$0")/../.."

pause() { echo "    sleeping ${1}s..."; sleep "$1"; }
hr() { printf '%.0s-' {1..70}; echo; }

experiment() {
  local svc="$1" wait_kill="$2" wait_recover="$3"
  hr; echo "CHAOS: killing '$svc'"
  docker compose kill "$svc" || true
  pause "$wait_kill"
  echo "    restarting '$svc'..."
  docker compose up -d "$svc"
  pause "$wait_recover"
  docker compose ps "$svc"
}

echo "Baseline DLQ depth (orders.dlq):"
docker compose exec -T kafka /opt/kafka/bin/kafka-run-class.sh kafka.tools.GetOffsetShell \
  --broker-list kafka:9092 --topic orders.dlq 2>/dev/null || echo "  (topic not created yet)"

experiment flink-taskmanager 10 30   # Flink should resume from checkpoint
experiment minio 8 25                # object store blip; REST catalog retries
experiment kafka 12 40               # broker restart; producers/consumers reconnect

hr
echo "Post-chaos smoke test:"
python3 tests/integration/smoke_test.py || echo "smoke reported issues (review above)"

echo "DLQ depth after chaos (should be >= baseline):"
docker compose exec -T kafka /opt/kafka/bin/kafka-run-class.sh kafka.tools.GetOffsetShell \
  --broker-list kafka:9092 --topic orders.dlq 2>/dev/null || true
hr
echo "Chaos run complete."
