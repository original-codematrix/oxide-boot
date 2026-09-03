#!/usr/bin/env bash
#
# Load test of the `showcase` example against the full local stack.
#
# Nothing here touches Kubernetes. The backing services (Redpanda, MongoDB,
# Dragonfly) and the observability stack (Prometheus, Grafana) run under
# `docker compose` on the local Docker daemon; the service itself runs on the
# host from a release build, so a code change is a restart and not an image
# build.
#
# Requires: docker (with compose), k6. Usage: make loadtest-showcase

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

COMPOSE="docker compose -f $ROOT/deploy/docker-compose.yml"
APP_PORT="${APP_PORT:-8080}"
MGMT_PORT="${MGMT_PORT:-8081}"
VUS="${VUS:-50}"
DURATION="${DURATION:-60s}"
OUT="${OUT:-$ROOT/target/loadtest-showcase}"
KEEP_STACK="${KEEP_STACK:-1}"

command -v docker >/dev/null || { echo "missing required tool: docker" >&2; exit 1; }
command -v k6 >/dev/null || { echo "missing required tool: k6 (brew install k6)" >&2; exit 1; }

mkdir -p "$OUT"
APP_PID=""

cleanup() {
  local rc=$?
  if [[ -n "$APP_PID" ]] && kill -0 "$APP_PID" 2>/dev/null; then
    # SIGTERM, not SIGKILL: this is the graceful-shutdown path, and the log
    # tail printed below is part of what the run is meant to show.
    kill -TERM "$APP_PID" 2>/dev/null || true
    wait "$APP_PID" 2>/dev/null || true
  fi
  if [[ "$KEEP_STACK" != "1" ]]; then
    $COMPOSE down -v >/dev/null 2>&1 || true
  fi
  exit $rc
}
trap cleanup EXIT INT TERM

echo "==> building showcase (release)"
cargo build --release -p showcase

echo "==> stack"
$COMPOSE up -d
echo "    waiting for Redpanda, MongoDB and Dragonfly to report healthy"
for _ in $(seq 1 90); do
  unhealthy=$($COMPOSE ps --format '{{.Service}} {{.Health}}' 2>/dev/null \
    | awk '$2 != "healthy" && $2 != "" { print $1 }' | tr '\n' ' ')
  [[ -z "$unhealthy" ]] && break
  sleep 2
done
if [[ -n "${unhealthy:-}" ]]; then
  echo "these services never became healthy: $unhealthy" >&2
  $COMPOSE ps >&2
  exit 1
fi
echo "    stack ready"

# The ports are offset from the test fixtures' so this stack can run next to
# `cargo test`; the app is pointed at them via env overrides rather than by
# editing application.yml.
echo "==> starting showcase on :${APP_PORT} (management :${MGMT_PORT})"
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

for _ in $(seq 1 60); do
  if curl -fsS "http://127.0.0.1:${MGMT_PORT}/actuator/health" >/dev/null 2>&1; then break; fi
  kill -0 "$APP_PID" 2>/dev/null || { echo "app exited during startup:" >&2; tail -40 "$OUT/app.log" >&2; exit 1; }
  sleep 1
done
health=$(curl -fsS "http://127.0.0.1:${MGMT_PORT}/actuator/health" 2>/dev/null || true)
[[ -n "$health" ]] || { echo "app did not become healthy within 60s" >&2; tail -40 "$OUT/app.log" >&2; exit 1; }
echo "    healthy: $health"

echo
echo "==> Grafana: http://localhost:3000/d/oxide-showcase (anonymous admin, no login)"
echo "==> k6: ${VUS} VUs for ${DURATION}"
echo

BASE_URL="http://127.0.0.1:${APP_PORT}" \
MGMT_URL="http://127.0.0.1:${MGMT_PORT}" \
VUS="$VUS" DURATION="$DURATION" \
K6_PROMETHEUS_RW_SERVER_URL="http://localhost:9090/api/v1/write" \
K6_PROMETHEUS_RW_TREND_STATS="p(50),p(95),p(99)" \
  k6 run -o experimental-prometheus-rw \
    --summary-export "$OUT/summary.json" \
    "$ROOT/scripts/loadtest-showcase.js" "$@" || K6_RC=$?
K6_RC="${K6_RC:-0}"

echo
echo "==> service counters (/actuator/prometheus)"
curl -fsS "http://127.0.0.1:${MGMT_PORT}/actuator/prometheus" \
  | grep -E '^showcase_' | head -20 || echo "    (no showcase_* series)"

echo
echo "==> /api/stats (producer -> Kafka -> listener -> Redis round trip)"
curl -fsS "http://127.0.0.1:${APP_PORT}/api/stats" || true
echo

echo
echo "==> app log tail"
tail -15 "$OUT/app.log"

echo
echo "artifacts: $OUT"
echo "the stack is still running (KEEP_STACK=0 to tear it down); Grafana keeps 6h of history"
exit "$K6_RC"
