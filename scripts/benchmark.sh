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
# 18080/18081, not the framework defaults: the test suite boots apps that bind
# 8080/8081, so a running stack must not sit on them.
APP_PORT="${APP_PORT:-18080}"
MGMT_PORT="${MGMT_PORT:-18081}"
KAFKA_SEED="${KAFKA_SEED:-200000}"
MONGO_SEED="${MONGO_SEED:-50000}"
VUS="${VUS:-100}"
# CONCURRENT=1 starts the HTTP load while the Kafka backlog is still draining,
# so both paths are saturated at once. That is the only way to measure combined
# capacity: with a fully drained backlog the consumer merely keeps pace with
# whatever the load test produces, which measures offered load, not capacity.
CONCURRENT="${CONCURRENT:-0}"
# IN_NETWORK=1 runs the service as a compose service instead of on the host, so
# its Redis/Mongo/Kafka traffic never crosses Docker Desktop's host-to-VM
# boundary. Only the HTTP port is published, for k6.
IN_NETWORK="${IN_NETWORK:-0}"
# K6_IN_NETWORK=1 runs the load generator as a container on the compose
# network, so requests no longer cross the host boundary either. On the
# cache-hit route that alone was worth +59% (17.2k -> 27.4k req/s). Requires
# IN_NETWORK=1, since it addresses the service by its compose name.
K6_IN_NETWORK="${K6_IN_NETWORK:-0}"
# Messages per second injected into the topic *during* the load phase, on top
# of what the REST writes produce. This is what makes the lag scenario
# realistic: a backlog that is only ever drained down is not the situation a
# consumer actually faces, since production does not stop while it catches up.
# 0 disables it.
PRODUCE_DURING_LOAD="${PRODUCE_DURING_LOAD:-0}"
# How much of the seeded backlog to drain before the load phase starts. 100
# means drain it fully (the isolated consumer measurement); a value below 100
# leaves a real lag standing when the HTTP load arrives.
DRAIN_PERCENT="${DRAIN_PERCENT:-100}"
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
  if [[ "$IN_NETWORK" == "1" ]]; then
    # --force-recreate: the service's Prometheus counters are cumulative over
    # the container's lifetime, so reusing a running container carries the
    # previous run's totals into this one's deltas.
    $COMPOSE --profile in-network up -d --build --force-recreate showcase >/dev/null
    for _ in $(seq 1 120); do
      curl -fsS "http://127.0.0.1:${MGMT_PORT}/actuator/health" >/dev/null 2>&1 && return 0
      sleep 1
    done
    echo "in-network service did not become healthy within 120s" >&2
    $COMPOSE logs --tail=40 showcase >&2
    exit 1
  fi
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
# A service left running from a previous run still holds the consumer group,
# which makes deleting and recreating the topic fail.
$COMPOSE --profile in-network rm -sf showcase >/dev/null 2>&1 || true
$COMPOSE exec -T mongo mongosh --quiet --eval 'db.getSiblingDB("showcase").notes.drop()' >/dev/null 2>&1 || true
# Plain `rpk`: the broker runs a separate internal listener advertised as
# `redpanda:9092`, so in-container clients resolve an address that exists on
# the compose network (see docker-compose.yml).
RPK="$COMPOSE exec -T redpanda rpk"
# The consumer group has to go with the topic. Leaving it behind means the
# group still holds committed offsets for the *deleted* topic's partitions;
# the next commit against the recreated (empty) partitions then crashes
# Redpanda v24.2.1 outright -- "Assert failure: Empty batch written to
# __consumer_offsets", exit 133. Reproduced twice.
$RPK group delete showcase >/dev/null 2>&1 || true
$RPK topic delete notes-events >/dev/null 2>&1 || true
# 6 partitions: the listener's default concurrency is 4, and a single-partition
# topic would cap the consumer at one in-order stream regardless of that.
# Idempotent: an "already exists" here is fine, an unreachable broker is not,
# so the topic is verified rather than trusting the exit code.
$RPK topic create notes-events -p 6 >/dev/null 2>&1 || true
$RPK topic list 2>/dev/null | grep -q '^notes-events' \
  || { echo "could not create the notes-events topic" >&2; $RPK cluster info >&2; exit 1; }
$COMPOSE exec -T dragonfly redis-cli FLUSHALL >/dev/null 2>&1 || true

echo
echo "=== phase 1: seed ==================================================="
./target/release/seed --kafka "$KAFKA_SEED" --mongo "$MONGO_SEED" 2>&1 | tee "$OUT/seed.log" | grep -E '^\[seed\] (kafka|mongo):'

echo
echo "=== phase 2: drain (consumer throughput, no HTTP load) =============="
start_app

if [[ "$CONCURRENT" == "1" ]]; then
  echo "    CONCURRENT=1: skipping the isolated drain, going straight to combined load"
  DRAIN_RATE=0
  drain_consumed=$(counter showcase_events_consumed_total); drain_consumed=${drain_consumed%.*}
else
drain_start=$(date +%s)
drain_target=$(( KAFKA_SEED * DRAIN_PERCENT / 100 ))
[[ "$DRAIN_PERCENT" -lt 100 ]] && echo "    draining to ${DRAIN_PERCENT}% ($drain_target of $KAFKA_SEED), leaving a lag standing"
prev=0
stable=0
while :; do
  consumed=$(counter showcase_events_consumed_total)
  consumed=${consumed%.*}
  now=$(date +%s)
  elapsed=$(( now - drain_start ))
  if [[ "$consumed" -ge "$drain_target" ]]; then
    echo "    drained $consumed/$KAFKA_SEED (target $drain_target) in ${elapsed}s"
    break
  fi
  # No progress for 30 consecutive seconds means the listener is wedged; a
  # benchmark that hangs here forever tells you nothing.
  if [[ "$consumed" == "$prev" ]]; then
    stable=$(( stable + 1 ))
    [[ "$stable" -ge 30 ]] && { echo "    consumer stalled at $consumed/$KAFKA_SEED after ${elapsed}s" >&2; break; }
  else
    stable=0
    printf "\r    %s/%s (%ss)" "$consumed" "$drain_target" "$elapsed"
  fi
  prev=$consumed
  [[ "$elapsed" -gt 900 ]] && { echo; echo "    giving up after 15 minutes" >&2; break; }
  sleep 1
done
drain_elapsed=$(( $(date +%s) - drain_start ))
drain_consumed=$(counter showcase_events_consumed_total); drain_consumed=${drain_consumed%.*}
DRAIN_RATE=$(( drain_elapsed > 0 ? drain_consumed / drain_elapsed : 0 ))
echo "    consumer throughput: ~${DRAIN_RATE} msg/s"
fi

# Samples the app process once a second for the duration of the load phase.
# `ps` reports %cpu as a share of ONE core, so on an N-core box the ceiling is
# N*100; rss is in kilobytes.
sample_resources() {
  local pid=$1 out=$2
  echo "cpu_pct rss_kb threads" > "$out"
  while kill -0 "$pid" 2>/dev/null; do
    # LC_ALL=C: under a comma-decimal locale ps prints "36,7" and every
    # consumer of this file has to guess which separator it got.
    LC_ALL=C ps -o %cpu=,rss=,wq= -p "$pid" 2>/dev/null | awk '{print $1, $2, $3}' >> "$out"
    sleep 1
  done
}

# The backing services are where the load actually lands, and reporting only
# the service process makes the system look almost idle when it is not.
sample_containers() {
  local out=$1
  echo "name cpu_pct mem_mb" > "$out"
  while :; do
    # The unit has to be converted, not stripped: "2GiB" and "2MiB" both
    # become "2" otherwise, which reported a 2 GB Prometheus as 2 MB.
    # LC_ALL applies to awk too, not just docker: under a comma-decimal locale
    # awk's printf writes "2048,0" and the reader parses it as 2048.
    LC_ALL=C docker stats --no-stream --format '{{.Name}} {{.CPUPerc}} {{.MemUsage}}' 2>/dev/null \
      | LC_ALL=C awk '{
            gsub(/%/, "", $2)
            mem = $3
            unit = mem
            gsub(/[0-9.]/, "", unit)
            gsub(/[^0-9.]/, "", mem)
            if (unit == "GiB") mem *= 1024
            else if (unit == "KiB") mem /= 1024
            else if (unit == "B") mem /= 1048576
            printf "%s %s %.1f\n", $1, $2, mem
        }' >> "$out"
    sleep 2
  done
}

echo
echo "=== phase 3: load (k6 over REST + Mongo + Redis + Kafka) ============"
# Truncate first: otherwise an IN_NETWORK run reports the previous host run's
# samples as if they were its own.
: > "$OUT/resources.txt"
if [[ -n "$APP_PID" ]]; then
  sample_resources "$APP_PID" "$OUT/resources.txt" &
  SAMPLER_PID=$!
fi
sample_containers "$OUT/containers.txt" &
CONTAINER_SAMPLER_PID=$!

PRODUCER_PID=""
if [[ "$PRODUCE_DURING_LOAD" != "0" ]]; then
  # Runs for longer than k6 does (ramp up + hold + ramp down + slack), so
  # production never stops before the load does.
  produce_seconds=$(( $(echo "$DURATION" | tr -dc '0-9') + 40 ))
  echo "    injecting ${PRODUCE_DURING_LOAD} msg/s into notes-events for ${produce_seconds}s alongside the load"
  ./target/release/seed --rate "$PRODUCE_DURING_LOAD" --for-seconds "$produce_seconds" \
    > "$OUT/producer.log" 2>&1 &
  PRODUCER_PID=$!
fi
echo "    Grafana: http://localhost:3000/d/oxide-showcase"
before_consumed=$drain_consumed
before_ts=$(date +%s)

if [[ "$K6_IN_NETWORK" == "1" ]]; then
  [[ "$IN_NETWORK" == "1" ]] || { echo "K6_IN_NETWORK=1 requires IN_NETWORK=1" >&2; exit 1; }
  NET=$(docker inspect oxide-loadtest-showcase-1 --format '{{range $k,$v := .NetworkSettings.Networks}}{{$k}}{{end}}')
  docker run --rm --network "$NET" \
    -v "$ROOT/scripts:/scripts:ro" -v "$OUT:/out" \
    -e BASE_URL="http://showcase:8080" -e MGMT_URL="http://showcase:8081" \
    -e VUS="$VUS" -e DURATION="$DURATION" \
    -e K6_PROMETHEUS_RW_SERVER_URL="http://prometheus:9090/api/v1/write" \
    -e K6_PROMETHEUS_RW_TREND_STATS="p(50),p(95),p(99)" \
    -e K6_PROMETHEUS_RW_PUSH_INTERVAL="10s" \
    grafana/k6:latest run -o experimental-prometheus-rw \
      --summary-export /out/summary.json \
      /scripts/loadtest-showcase.js "$@" 2>&1 | tee "$OUT/k6.log" || true
else
BASE_URL="http://127.0.0.1:${APP_PORT}" \
MGMT_URL="http://127.0.0.1:${MGMT_PORT}" \
VUS="$VUS" DURATION="$DURATION" \
K6_PROMETHEUS_RW_SERVER_URL="http://localhost:9090/api/v1/write" \
K6_PROMETHEUS_RW_TREND_STATS="p(50),p(95),p(99)" \
K6_PROMETHEUS_RW_PUSH_INTERVAL="10s" \
  k6 run -o experimental-prometheus-rw \
    --summary-export "$OUT/summary.json" \
    "$ROOT/scripts/loadtest-showcase.js" "$@" 2>&1 | tee "$OUT/k6.log" || true
fi

if [[ -n "$PRODUCER_PID" ]]; then
  wait "$PRODUCER_PID" 2>/dev/null || true
  grep -E '^\[seed\] steady producer' "$OUT/producer.log" | sed 's/^/  /' || true
fi
kill ${SAMPLER_PID:-} "$CONTAINER_SAMPLER_PID" 2>/dev/null || true

after_consumed=$(counter showcase_events_consumed_total); after_consumed=${after_consumed%.*}
after_produced=$(counter showcase_notes_created_total); after_produced=${after_produced%.*}
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
# Whether the consume rate is a capacity measurement depends on one thing
# only: was there a backlog for the consumer to eat during the load phase?
# Ask the broker rather than inferring it from which flags were set.
TOTAL_LAG=$($RPK group describe showcase 2>/dev/null \
  | awk '$1 == "notes-events" { sum += $5 } END { print sum + 0 }')
[[ "$DRAIN_PERCENT" -lt 100 ]] && echo "  Kafka consume rate, idle ..... ~${DRAIN_RATE} msg/s  (drained to ${DRAIN_PERCENT}%)"
[[ "$DRAIN_PERCENT" -ge 100 && "$CONCURRENT" != "1" ]] && echo "  Kafka consume rate, idle ..... ~${DRAIN_RATE} msg/s"
if [[ "${TOTAL_LAG:-0}" -gt 1000 ]]; then
  echo "  Kafka consumed during load ... ~${LOAD_CONSUME_RATE} msg/s  (${TOTAL_LAG} still lagging:"
  echo "                                  the consumer was never idle, so this IS its capacity)"
else
  echo "  Kafka consumed during load ... ~${LOAD_CONSUME_RATE} msg/s  (lag ${TOTAL_LAG:-0}: the consumer"
  echo "                                  kept up, so this tracks the produce rate, not capacity)"
fi
echo
if [[ "$IN_NETWORK" == "1" ]]; then
  echo "  service runs in-network; see the showcase container in the table below"
else
  echo "  resource use (host service process):"
fi
python3 - "$OUT/resources.txt" "$(sysctl -n hw.ncpu 2>/dev/null || nproc)" <<'PY2'
import sys
try:
    rows = [l.split() for l in open(sys.argv[1]).read().splitlines()[1:] if l.strip()]
    cpu = [float(r[0]) for r in rows if len(r) >= 2]
    rss = [int(r[1]) for r in rows if len(r) >= 2]
except Exception as e:
    print(f"    (no samples: {e})"); raise SystemExit
if not cpu:
    print("    (no samples)"); raise SystemExit
cores = int(sys.argv[2])
# ps reports %cpu against one core, so divide by the core count to get the
# share of the whole machine.
print(f"    CPU avg / peak ............. {sum(cpu)/len(cpu):.0f}% / {max(cpu):.0f}% of one core"
      f"  ({sum(cpu)/len(cpu)/cores:.0f}% / {max(cpu)/cores:.0f}% of {cores} cores)")
print(f"    RSS avg / peak ............. {sum(rss)/len(rss)/1024:.0f} MB / {max(rss)/1024:.0f} MB")
print(f"    samples .................... {len(cpu)} (1/s)")
PY2

echo
echo "  consumer lag per partition at the broker (authoritative, not inferred):"
$RPK group describe showcase 2>/dev/null \
  | awk '/PARTITION/{p=1} p{print "    " $0}' | head -12 || echo "    (unavailable)"

echo
echo "  backing services (docker stats, %CPU is of one core):"
python3 - "$OUT/containers.txt" <<'PY3'
import sys
from collections import defaultdict
try:
    lines = open(sys.argv[1]).read().splitlines()[1:]
except Exception as e:
    print(f"    (no samples: {e})"); raise SystemExit
cpu, mem = defaultdict(list), defaultdict(list)
for l in lines:
    p = l.split()
    if len(p) < 3: continue
    try:
        cpu[p[0]].append(float(p[1])); mem[p[0]].append(float(p[2]))
    except ValueError:
        continue
if not cpu:
    print("    (no samples)"); raise SystemExit
for name in sorted(cpu, key=lambda n: -max(cpu[n])):
    c, m = cpu[name], mem[name]
    print(f"    {name:34s} CPU avg {sum(c)/len(c):6.0f}%  peak {max(c):6.0f}%   mem {max(m):6.0f} MB")
PY3

echo
echo "  service counters:"
curl -fsS "http://127.0.0.1:${MGMT_PORT}/actuator/prometheus" | grep -E '^showcase_' | sed 's/^/    /'
echo
echo "  /api/stats: $(curl -fsS "http://127.0.0.1:${APP_PORT}/api/stats" || echo unavailable)"
echo
echo "artifacts: $OUT   (stack left running; make stack-down to remove it)"
