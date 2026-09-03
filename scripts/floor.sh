#!/usr/bin/env bash
#
# Measures the framework's HTTP ceiling with no backing services in the path,
# using the rest-api example's in-memory handler. This is the number every
# other benchmark here should be read against: if a route is far below it, the
# cost is in what that route talks to, not in the framework.
#
# Usage: make floor

set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

PORT="${PORT:-8090}"
MGMT="${MGMT:-8091}"
VUS="${VUS:-100}"
DURATION="${DURATION:-20s}"

command -v k6 >/dev/null || { echo "missing required tool: k6" >&2; exit 1; }

APP_PID=""
cleanup() { [[ -n "$APP_PID" ]] && kill -TERM "$APP_PID" 2>/dev/null && wait "$APP_PID" 2>/dev/null; }
trap cleanup EXIT INT TERM

cargo build --release -p rest-api 2>&1 | tail -1

OXIDE_SERVER_PORT="$PORT" OXIDE_MANAGEMENT_PORT="$MGMT" OXIDE_LOGGING_LEVEL_ROOT=warn \
  sh -c 'cd "$1" && exec "$2"' _ "$ROOT/examples/rest-api" "$ROOT/target/release/rest-api" \
  > "$ROOT/target/floor-app.log" 2>&1 &
APP_PID=$!

for _ in $(seq 1 60); do
  curl -fsS "http://127.0.0.1:${MGMT}/actuator/health" >/dev/null 2>&1 && break
  sleep 1
done

echo "==> GET /api/notes (in-memory handler, no Redis/Mongo/Kafka in the path)"
URL="http://127.0.0.1:${PORT}/api/notes" VUS="$VUS" DURATION="$DURATION" \
  k6 run "$ROOT/scripts/single-route.js"
