#!/usr/bin/env bash
#
# What this service does on its own, and what it does when everything runs at
# once -- with the resource cost of each.
#
# Three scenarios, identical in every respect except which load is applied, so
# the numbers are comparable to each other rather than to a different rig:
#
#   http    HTTP only. No Kafka backlog, no producer. What the REST surface
#           does when the consumer is idle.
#   kafka   Kafka only. A backlog left standing, no HTTP, no producer. What
#           the consumer does when nothing competes with it.
#   both    Backlog standing, HTTP load, and a producer injecting throughout.
#           The number that matters for capacity planning.
#
# Each scenario's resource use is sampled during its load phase only, so the
# CPU and memory figures belong to that scenario and not to the seeding or the
# drain that preceded it.
#
# Requires the stack (`make stack`) and a built image. Usage: make capacity

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

OUT="${OUT:-$ROOT/target/capacity}"
VUS="${VUS:-100}"
DURATION="${DURATION:-45s}"
BACKLOG="${BACKLOG:-300000}"
CORPUS="${CORPUS:-50000}"
INJECT="${INJECT:-2000}"

mkdir -p "$OUT"

# A benchmark on a busy machine measures the machine, not the service. This
# has already produced one thoroughly misleading run: with an unrelated game
# server, a game client and a recursive grep competing for the same 12 cores,
# the HTTP figure came out at 980 req/s against 17,700 on the same commit
# minutes earlier.
CORES=$(sysctl -n hw.ncpu 2>/dev/null || nproc)
LOAD=$(LC_ALL=C uptime | sed 's/.*averages*: *//' | awk '{print $1}' | tr ',' '.')
BUSY=$(LC_ALL=C awk -v l="$LOAD" -v c="$CORES" 'BEGIN { print (l > c * 0.3) ? 1 : 0 }')
if [[ "$BUSY" == "1" ]]; then
  echo "WARNING: load average is $LOAD on $CORES cores before this run even starts." >&2
  echo "         Whatever else is running will be measured along with the service." >&2
  LC_ALL=C ps -Ao pcpu,comm -r 2>/dev/null | head -6 | sed 's/^/         /' >&2
  echo "         Set ALLOW_BUSY=1 to run anyway." >&2
  [[ "${ALLOW_BUSY:-0}" == "1" ]] || exit 1
fi

# Runs one scenario through the benchmark and pulls its numbers back out.
# `DRAIN_PERCENT=10` leaves ~90% of the backlog standing, so the consumer is
# still working when the load phase begins -- otherwise its "rate" would just
# be whatever was offered.
scenario() {
  local name=$1 seed=$2 drain=$3 inject=$4 vus=$5 no_http=${6:-0}
  echo "==> $name" >&2
  IN_NETWORK=1 K6_IN_NETWORK=1 CONCURRENT=0 NO_HTTP="$6" \
  KAFKA_SEED="$seed" MONGO_SEED="$CORPUS" DRAIN_PERCENT="$drain" \
  PRODUCE_DURING_LOAD="$inject" VUS="$vus" DURATION="$DURATION" \
  OUT="$OUT/$name" \
    "$ROOT/scripts/benchmark.sh" > "$OUT/$name.log" 2>&1 || true
}

# Every field is optional: a scenario with no HTTP has no k6 summary, and one
# with no backlog has no consume rate. Missing prints as "-" rather than 0,
# which would read as "measured zero".
field() {
  local file=$1 pattern=$2
  grep -oE "$pattern" "$file" 2>/dev/null | tail -1 | grep -oE '[0-9]+(\.[0-9]+)?$' || echo "-"
}

resources() {
  local name=$1
  LC_ALL=C awk '
    $1 == "oxide-loadtest-showcase-1" { c[++n] = $2; m[n] = $3 }
    END {
      if (n == 0) { print "- - -"; exit }
      s = 0; mx = 0; mm = 0
      for (i = 1; i <= n; i++) { s += c[i]; if (c[i] > mx) mx = c[i]; if (m[i] > mm) mm = m[i] }
      printf "%.2f %.2f %.0f\n", s/n/100, mx/100, mm
    }' "$OUT/$name/containers.txt" 2>/dev/null || echo "- - -"
}

scenario http  0          100 0         "$VUS" 0
# NO_HTTP rather than VUS=0: k6 with zero VUs exits at once, which collapses
# the measurement window instead of leaving the consumer alone in it.
scenario kafka "$BACKLOG" 10  0         0      1
scenario both  "$BACKLOG" 10  "$INJECT" "$VUS" 0

echo
echo "==================================================================================="
echo " Capacity, one replica, service and backing services in one docker network"
echo "==================================================================================="
printf "\n%-8s %-13s %-11s %-13s %-11s %-9s %s\n" \
  "scenario" "HTTP req/s" "p50/p95 ms" "Kafka msg/s" "CPU avg" "CPU peak" "RSS peak"
printf "%s\n" "-----------------------------------------------------------------------------------"
for s in http kafka both; do
  f="$OUT/$s.log"
  http=$(field "$f" 'HTTP requests/s \.+ [0-9.]+')
  p50=$(grep -oE 'HTTP p50 / p95 / p99 \(ms\) \.+ [0-9.]+ / [0-9.]+' "$f" 2>/dev/null | awk '{print $(NF-2)" / "$NF}' || echo "-")
  kafka=$(field "$f" 'consumed during load \.+ ~[0-9]+')
  read -r cavg cpeak mpeak <<<"$(resources "$s")"
  printf "%-8s %-13s %-11s %-13s %-11s %-9s %s MB\n" \
    "$s" "$http" "${p50:--}" "$kafka" "${cavg} cores" "${cpeak}" "${mpeak}"
done

echo
echo "The http row's Kafka figure is not a backlog being drained -- there is no"
echo "backlog in that scenario. It is the events the REST writes themselves"
echo "produce, consumed as fast as they arrive."
echo
echo "Ceilings measured separately (see README):"
echo "  framework floor, in-memory handler, no backend ....... ~145,000 req/s @ p50 0.41 ms"
echo "  one cache-hit route, in-network ...................... ~27,400 req/s @ p50 0.67 ms"
echo "  Kafka producer into the broker (seed) ................ ~168,000 msg/s"
echo
echo "logs: $OUT"
