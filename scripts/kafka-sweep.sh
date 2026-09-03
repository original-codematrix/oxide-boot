#!/usr/bin/env bash
#
# Sweeps the Kafka listener's `concurrency` setting against a fixed backlog,
# with no HTTP load, to find what actually caps consumer throughput.
#
# `oxide.kafka.consumer.concurrency` bounds records in flight across *all*
# partitions (a partition is still processed strictly sequentially). The
# showcase listener does one Redis round trip per message, so if throughput
# scales with this number the cap is that round trip, and if it flattens the
# cap is somewhere else -- the fetch path, the commit path, or the broker.
#
# Usage: scripts/kafka-sweep.sh [concurrency values...]   (default: 4 16 64 256)

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

COMPOSE="docker compose -f $ROOT/deploy/docker-compose.yml"
MGMT_PORT="${MGMT_PORT:-18081}"
# 18080/18081, not the framework defaults: the test suite boots apps that bind
# 8080/8081, so a running stack must not sit on them.
APP_PORT="${APP_PORT:-18080}"
BACKLOG="${BACKLOG:-200000}"
PARTITIONS="${PARTITIONS:-6}"
OUT="${OUT:-$ROOT/target/kafka-sweep}"
VALUES=("${@:-}")
[[ -z "${VALUES[0]:-}" ]] && VALUES=(4 16 64 256)

mkdir -p "$OUT"
APP_PID=""
cleanup() {
  local rc=$?
  [[ -n "$APP_PID" ]] && kill -TERM "$APP_PID" 2>/dev/null && wait "$APP_PID" 2>/dev/null
  exit $rc
}
trap cleanup EXIT INT TERM

counter() {
  curl -fsS "http://127.0.0.1:${MGMT_PORT}/actuator/prometheus" 2>/dev/null \
    | awk -v m="$1" '$1 == m { print $2; found=1 } END { if (!found) print 0 }' | head -1
}

cargo build --release -p showcase 2>&1 | tail -1
$COMPOSE up -d >/dev/null
for _ in $(seq 1 60); do
  bad=$($COMPOSE ps --format '{{.Service}} {{.Health}}' | awk '$2 != "healthy" && $2 != "" {print $1}')
  [[ -z "$bad" ]] && break
  sleep 2
done

printf '\n%-14s %-12s %-10s %s\n' "concurrency" "drained" "seconds" "msg/s"
printf '%s\n' "------------------------------------------------------"

for c in "${VALUES[@]}"; do
  # Fresh topic and group each iteration: a leftover committed offset would
  # let the next run start part-way through the backlog.
  $COMPOSE exec -T redpanda rpk topic delete notes-events >/dev/null 2>&1 || true
  $COMPOSE exec -T redpanda rpk topic create notes-events -p "$PARTITIONS" >/dev/null
  $COMPOSE exec -T dragonfly redis-cli FLUSHALL >/dev/null 2>&1 || true
  ./target/release/seed --kafka "$BACKLOG" >/dev/null 2>&1

  OXIDE_SERVER_PORT="$APP_PORT" \
  OXIDE_MANAGEMENT_PORT="$MGMT_PORT" \
  OXIDE_KAFKA_BOOTSTRAP_SERVERS="localhost:19092" \
  OXIDE_MONGO_URI="mongodb://localhost:37017" \
  OXIDE_REDIS_URL="redis://127.0.0.1:16379" \
  OXIDE_KAFKA_CONSUMER_CONCURRENCY="$c" \
  OXIDE_LOGGING_LEVEL_ROOT=warn \
    sh -c 'cd "$1" && exec "$2"' _ \
      "$ROOT/examples/showcase" "$ROOT/target/release/showcase" \
      > "$OUT/app-c$c.log" 2>&1 &
  APP_PID=$!

  for _ in $(seq 1 60); do
    curl -fsS "http://127.0.0.1:${MGMT_PORT}/actuator/health" >/dev/null 2>&1 && break
    sleep 1
  done

  start=$(date +%s)
  while :; do
    n=$(counter showcase_events_consumed_total); n=${n%.*}
    el=$(( $(date +%s) - start ))
    [[ "$n" -ge "$BACKLOG" ]] && break
    [[ "$el" -gt 300 ]] && break
    sleep 1
  done
  el=$(( $(date +%s) - start ))
  n=$(counter showcase_events_consumed_total); n=${n%.*}
  rate=$(( el > 0 ? n / el : 0 ))
  printf '%-14s %-12s %-10s %s\n' "$c" "$n" "$el" "$rate"

  kill -TERM "$APP_PID" 2>/dev/null || true
  wait "$APP_PID" 2>/dev/null || true
  APP_PID=""
done

echo
echo "logs: $OUT"
