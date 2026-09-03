#!/usr/bin/env bash
#
# End-to-end load test of the rest-api example.
#
# Builds the example in release, brings up a Redpanda broker (the example's
# POST handler publishes to `notes-events`, so a load test without a broker
# would only measure the error path), waits for readiness the way a kubelet
# would, drives it with k6, and reports the framework's own counters from
# /actuator/prometheus alongside k6's client-side numbers.
#
# Requires: docker, k6. Usage: make loadtest [-- k6 args]

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

APP_PORT="${APP_PORT:-18080}"
MGMT_PORT="${MGMT_PORT:-18081}"
KAFKA_PORT="${KAFKA_PORT:-9092}"
CONTAINER="${CONTAINER:-oxide-loadtest-redpanda}"
VUS="${VUS:-50}"
DURATION="${DURATION:-30s}"
OUT="${OUT:-$ROOT/target/loadtest}"

for tool in docker k6; do
  command -v "$tool" >/dev/null || { echo "missing required tool: $tool" >&2; exit 1; }
done

mkdir -p "$OUT"
APP_PID=""
STARTED_CONTAINER=0

cleanup() {
  local rc=$?
  if [[ -n "$APP_PID" ]] && kill -0 "$APP_PID" 2>/dev/null; then
    # SIGTERM, not SIGKILL: this exercises the graceful-shutdown path and the
    # log tail below is part of what the load test is meant to show.
    kill -TERM "$APP_PID" 2>/dev/null || true
    wait "$APP_PID" 2>/dev/null || true
  fi
  if [[ "$STARTED_CONTAINER" == "1" ]]; then
    docker rm -f "$CONTAINER" >/dev/null 2>&1 || true
  fi
  exit $rc
}
trap cleanup EXIT INT TERM

echo "==> building rest-api (release)"
cargo build --release -p rest-api

echo "==> broker"
if docker inspect -f '{{.State.Running}}' "$CONTAINER" 2>/dev/null | grep -q true; then
  echo "    reusing running container $CONTAINER"
else
  docker rm -f "$CONTAINER" >/dev/null 2>&1 || true
  # Mirrors crates/oxide-test/src/containers.rs so the load test and the test
  # suite exercise the same broker configuration.
  docker run -d --name "$CONTAINER" -p "${KAFKA_PORT}:9092" \
    redpandadata/redpanda:v24.2.1 \
    redpanda start --mode dev-container --overprovisioned \
    --smp 2 --memory 2G \
    --kafka-addr 0.0.0.0:9092 \
    --advertise-kafka-addr "localhost:${KAFKA_PORT}" \
    --set redpanda.empty_seed_starts_cluster=true >/dev/null
  STARTED_CONTAINER=1
  echo "    started $CONTAINER on :${KAFKA_PORT}"
fi

for _ in $(seq 1 60); do
  if docker exec "$CONTAINER" rpk cluster info >/dev/null 2>&1; then break; fi
  sleep 1
done
docker exec "$CONTAINER" rpk cluster info >/dev/null 2>&1 \
  || { echo "broker did not become ready within 60s" >&2; exit 1; }
echo "    broker ready"

# `application.yml` is resolved relative to the working directory, so the
# binary has to run from the example's own directory, not from the repo root.
echo "==> starting rest-api on :${APP_PORT} (management :${MGMT_PORT})"
OXIDE_SERVER_PORT="$APP_PORT" \
OXIDE_MANAGEMENT_PORT="$MGMT_PORT" \
OXIDE_KAFKA_BOOTSTRAP_SERVERS="localhost:${KAFKA_PORT}" \
OXIDE_LOGGING_LEVEL_ROOT=warn \
  sh -c 'cd "$1" && exec "$2"' _ \
    "$ROOT/examples/rest-api" "$ROOT/target/release/rest-api" \
    > "$OUT/app.log" 2>&1 &
APP_PID=$!

for _ in $(seq 1 60); do
  if curl -fsS "http://127.0.0.1:${MGMT_PORT}/actuator/health" >/dev/null 2>&1; then break; fi
  kill -0 "$APP_PID" 2>/dev/null || { echo "app exited during startup:" >&2; tail -30 "$OUT/app.log" >&2; exit 1; }
  sleep 1
done
curl -fsS "http://127.0.0.1:${MGMT_PORT}/actuator/health" >/dev/null \
  || { echo "app did not become healthy within 60s" >&2; tail -30 "$OUT/app.log" >&2; exit 1; }
echo "    healthy"

echo "==> k6: ${VUS} VUs for ${DURATION}"
BASE_URL="http://127.0.0.1:${APP_PORT}" \
MGMT_URL="http://127.0.0.1:${MGMT_PORT}" \
VUS="$VUS" DURATION="$DURATION" \
  k6 run --summary-export "$OUT/summary.json" "$ROOT/scripts/loadtest.js" "$@"
K6_RC=$?

echo
echo "==> framework counters (/actuator/prometheus)"
curl -fsS "http://127.0.0.1:${MGMT_PORT}/actuator/prometheus" \
  | grep -E '^(notes_created_total|http_server_requests)' \
  | head -20 || echo "    (no matching series)"

echo
echo "==> app log tail"
tail -15 "$OUT/app.log"

echo
echo "artifacts: $OUT"
exit $K6_RC
