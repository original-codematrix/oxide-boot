#!/usr/bin/env bash
#
# Full benchmark of the `showcase` example: how much this framework actually
# moves, across all four integrations.
#
# The run is split into phases on purpose, because a single mixed run cannot
# tell you which component is the limit:
#
#   1. seed   -- fill Mongo with a corpus and Kafka with a backlog, before the
#                app exists. The producer rate printed here is the broker's
#                ceiling on this machine, which is the yardstick for phase 2.
#   2. drain  -- start the app and measure how fast the Kafka listener eats the
#                backlog with no HTTP load competing for the runtime. This is
#                consumer throughput, isolated.
#   3. load   -- k6 drives the REST surface (Mongo reads and writes, Redis
#                cache-aside, Kafka produces) while the consumer keeps running.
#                The drop from phase 2's drain rate is the cost of contention.
#
# Nothing here touches Kubernetes: the backing services run under docker
# compose on the local Docker daemon, and the service runs on the host.
#
# Requires: docker (with compose), k6. Usage: make benchmark

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

COMPOSE="docker compose -f $ROOT/deploy/docker-compose.yml"
APP_PORT="${APP_PORT:-8080}"
MGMT_PORT="${MGMT_PORT:-8081}"
KAFKA_SEED="${KAFKA_SEED:-200000}"
MONGO_SEED="${MONGO_SEED:-50000}"
VUS="${VUS:-100}"
DURATION="${DURATION:-60s}"
OUT="${OUT:-$ROOT/target/benchmark}"

command -v docker >/dev/null || { echo "missing required tool: docker" >&2; exit 1; }
command -v k6 >/dev/null || { echo "missing required tool: k6 (brew install k6)" >&2; exit 1; }

mkdir -p "$OUT"
APP_PID=""

cleanup() {
  local rc=$?
  if [[ -n "$APP_PID" ]] && kill -0 "$APP_PID" 2>/dev/null; then
    kill -TERM "$APP_PID" 2>/dev/null || true
    wait "$APP_PID" 2>/dev/null || true
  fi
  exit $rc
}
trap cleanup EXIT INT TERM

# Reads one `showcase_*` counter out of /actuator/prometheus. Absent means the
# metric has not been emitted yet, which is zero, not an error.
counter() {
  curl -fsS "http://127.0.0.1:${MGMT_PORT}/actuator/prometheus" 2>/dev/null \
    | awk -v m="$1" '$1 == m { print $2; found=1 } END { if (!found) print 0 }' \
    | head -1
}

start_app() {
  OXIDE_SERVER_PORT="$APP_PORT" \
  OXIDE_MANAGEMENT_PORT="$MGMT_PORT" \
  OXIDE_KAFKA_BOOTSTRAP_SERVERS="localhost:19092" \
  OXIDE_MONGO_URI="mongodb://localhost:37017" \
  OXIDE_REDIS_URL="redis://127.0.0.1:16379" \
  OXIDE_LOGGING_LEVEL_ROOT=warn \
    sh -c 'cd "$1" && exec "$2"' _ \
      "$ROOT/examples/showcase" "$ROOT/target/release/showcase" \
      > "$OUT/app.log" 2>&1 &
  APP_PID=$!
  for _ in $(seq 1 90); do
    curl -fsS "http://127.0.0.1:${MGMT_PORT}/actuator/health" >/dev/null 2>&1 && return 0
    kill -0 "$APP_PID" 2>/dev/null || { echo "app exited during startup:" >&2; tail -40 "$OUT/app.log" >&2; exit 1; }
    sleep 1
  done
  echo "app did not become healthy within 90s" >&2; tail -40 "$OUT/app.log" >&2; exit 1
}

echo "==> building (release)"
cargo build --release -p showcase

echo "==> stack"
$COMPOSE up -d >/dev/null
for _ in $(seq 1 90); do
  unhealthy=$($COMPOSE ps --format '{{.Service}} {{.Health}}' 2>/dev/null \
    | awk '$2 != "healthy" && $2 != "" { print $1 }' | tr '\n' ' ')
  [[ -z "$unhealthy" ]] && break
  sleep 2
done
[[ -z "${unhealthy:-}" ]] || { echo "never became healthy: $unhealthy" >&2; $COMPOSE ps >&2; exit 1; }

# A clean slate each run: a backlog left over from a previous benchmark would
# be drained during phase 2 and inflate the number.
echo "==> resetting state (mongo collection, kafka topic, redis counters)"
$COMPOSE exec -T mongo mongosh --quiet --eval 'db.getSiblingDB("showcase").notes.drop()' >/dev/null 2>&1 || true
# Plain `rpk`: the broker runs a separate internal listener advertised as
# `redpanda:9092`, so in-container clients resolve an address that exists on
# the compose network (see docker-compose.yml).
RPK="$COMPOSE exec -T redpanda rpk"
$RPK topic delete notes-events >/dev/null 2>&1 || true
# 6 partitions: the listener's default concurrency is 4, and a single-partition
# topic would cap the consumer at one in-order stream regardless of that.
$RPK topic create notes-events -p 6 >/dev/null
$COMPOSE exec -T dragonfly redis-cli FLUSHALL >/dev/null 2>&1 || true

echo
echo "=== phase 1: seed ==================================================="
./target/release/seed --kafka "$KAFKA_SEED" --mongo "$MONGO_SEED" 2>&1 | tee "$OUT/seed.log" | grep -E '^\[seed\] (kafka|mongo):'

echo
echo "=== phase 2: drain (consumer throughput, no HTTP load) =============="
start_app
drain_start=$(date +%s)
prev=0
stable=0
while :; do
  consumed=$(counter showcase_events_consumed_total)
  consumed=${consumed%.*}
  now=$(date +%s)
  elapsed=$(( now - drain_start ))
  if [[ "$consumed" -ge "$KAFKA_SEED" ]]; then
    echo "    drained $consumed/$KAFKA_SEED in ${elapsed}s"
    break
  fi
  # No progress for 30 consecutive seconds means the listener is wedged; a
  # benchmark that hangs here forever tells you nothing.
  if [[ "$consumed" == "$prev" ]]; then
    stable=$(( stable + 1 ))
    [[ "$stable" -ge 30 ]] && { echo "    consumer stalled at $consumed/$KAFKA_SEED after ${elapsed}s" >&2; break; }
  else
    stable=0
    printf "\r    %s/%s (%ss)" "$consumed" "$KAFKA_SEED" "$elapsed"
  fi
  prev=$consumed
  [[ "$elapsed" -gt 900 ]] && { echo; echo "    giving up after 15 minutes" >&2; break; }
  sleep 1
done
drain_elapsed=$(( $(date +%s) - drain_start ))
drain_consumed=$(counter showcase_events_consumed_total); drain_consumed=${drain_consumed%.*}
DRAIN_RATE=$(( drain_elapsed > 0 ? drain_consumed / drain_elapsed : 0 ))
echo "    consumer throughput: ~${DRAIN_RATE} msg/s"

echo
echo "=== phase 3: load (k6 over REST + Mongo + Redis + Kafka) ============"
echo "    Grafana: http://localhost:3000/d/oxide-showcase"
before_consumed=$drain_consumed
before_ts=$(date +%s)

BASE_URL="http://127.0.0.1:${APP_PORT}" \
MGMT_URL="http://127.0.0.1:${MGMT_PORT}" \
VUS="$VUS" DURATION="$DURATION" \
K6_PROMETHEUS_RW_SERVER_URL="http://localhost:9090/api/v1/write" \
K6_PROMETHEUS_RW_TREND_STATS="p(50),p(95),p(99)" \
K6_PROMETHEUS_RW_PUSH_INTERVAL="2s" \
  k6 run -o experimental-prometheus-rw \
    --summary-export "$OUT/summary.json" \
    "$ROOT/scripts/loadtest-showcase.js" "$@" 2>&1 | tee "$OUT/k6.log" || true

after_consumed=$(counter showcase_events_consumed_total); after_consumed=${after_consumed%.*}
after_ts=$(date +%s)
load_window=$(( after_ts - before_ts ))
LOAD_CONSUME_RATE=$(( load_window > 0 ? (after_consumed - before_consumed) / load_window : 0 ))

echo
echo "=== results ========================================================="
python3 - "$OUT/summary.json" <<'PY'
import json, sys
try:
    s = json.load(open(sys.argv[1]))
except Exception as e:
    print(f"  (no k6 summary: {e})")
    raise SystemExit
m = s.get("metrics", {})
def g(name, key, scale=1.0):
    v = m.get(name, {}).get(key)
    return f"{v*scale:.1f}" if isinstance(v, (int, float)) else "n/a"
print(f"  HTTP requests/s .............. {g('http_reqs','rate')}")
print(f"  HTTP p50 / p95 / p99 (ms) ... {g('http_req_duration','med')} / {g('http_req_duration','p(95)')} / {g('http_req_duration','p(99)')}")
print(f"  HTTP failure rate ............ {g('http_req_failed','rate',100)}%")
print(f"  Iterations/s ................. {g('iterations','rate')}")
PY
echo "  Kafka consume rate, idle ..... ~${DRAIN_RATE} msg/s"
echo "  Kafka consume rate, loaded ... ~${LOAD_CONSUME_RATE} msg/s"
echo
echo "  service counters:"
curl -fsS "http://127.0.0.1:${MGMT_PORT}/actuator/prometheus" | grep -E '^showcase_' | sed 's/^/    /'
echo
echo "  /api/stats: $(curl -fsS "http://127.0.0.1:${APP_PORT}/api/stats" || echo unavailable)"
echo
echo "artifacts: $OUT   (stack left running; make stack-down to remove it)"
