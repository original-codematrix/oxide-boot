#!/usr/bin/env bash
#
# Drives the service to saturation and records where it gets there.
#
# A single load level says almost nothing on its own: 15,000 req/s at 100 VUs
# could mean the service is at its limit or that the load generator simply did
# not ask for more. This ramps the concurrency until throughput stops rising
# and latency starts to, which is the only way to tell those apart -- and the
# knee is the number a capacity plan is actually built on.
#
# Every level runs the full stack: HTTP load, a standing Kafka backlog, and a
# producer injecting throughout, so nothing is measured in an unrealistically
# quiet system.
#
# Usage: make saturation   (or: scripts/saturation.sh 25 50 100 200 400)

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

OUT="${OUT:-$ROOT/target/saturation}"
DURATION="${DURATION:-45s}"
BACKLOG="${BACKLOG:-300000}"
CORPUS="${CORPUS:-50000}"
INJECT="${INJECT:-2000}"
LEVELS=("$@")
[[ ${#LEVELS[@]} -eq 0 ]] && LEVELS=(25 50 100 200 400 800)

mkdir -p "$OUT"

CORES=$(sysctl -n hw.ncpu 2>/dev/null || nproc)
LOAD=$(LC_ALL=C uptime | sed 's/.*averages*: *//' | awk '{print $1}' | tr ',' '.')
BUSY=$(LC_ALL=C awk -v l="$LOAD" -v c="$CORES" 'BEGIN { print (l > c * 0.4) ? 1 : 0 }')
if [[ "$BUSY" == "1" && "${ALLOW_BUSY:-0}" != "1" ]]; then
  echo "load average $LOAD on $CORES cores: too busy to measure saturation." >&2
  echo "Set ALLOW_BUSY=1 to override, but the numbers will be the machine's, not the service's." >&2
  exit 1
fi

# The report has to carry the conditions it was measured under, or a number
# from a contended machine is indistinguishable from the service's limit.
{
  echo "cores:      $CORES"
  echo "load start: $LOAD"
  sw_vers 2>/dev/null | tr '\n' ' ' || true
  echo
  echo "competing processes at start (%CPU, RSS MB, command):"
  { LC_ALL=C ps -Ao pcpu,rss,comm -r 2>/dev/null \
      | awk 'NR>1 && $1 > 5 { printf "  %6.1f%%  %6.0f MB  %s\n", $1, $2/1024, $3 }' \
      | head -8; } || true
} | tee "$OUT/context.txt"

num() { grep -oE "$2" "$1" 2>/dev/null | tail -1 | grep -oE '[0-9]+(\.[0-9]+)?$' || echo "-"; }

for vus in "${LEVELS[@]}"; do
  echo "==> $vus VUs" >&2
  IN_NETWORK=1 K6_IN_NETWORK=1 CONCURRENT=0 DRAIN_PERCENT=10 \
  KAFKA_SEED="$BACKLOG" MONGO_SEED="$CORPUS" PRODUCE_DURING_LOAD="$INJECT" \
  VUS="$vus" DURATION="$DURATION" OUT="$OUT/v$vus" \
    "$ROOT/scripts/benchmark.sh" > "$OUT/v$vus.log" 2>&1 || true
done

printf "\n%-6s %-11s %-8s %-8s %-8s %-8s %-9s %-10s %-9s %s\n" \
  "VUs" "req/s" "p50" "p95" "p99" "max" "errors" "Kafka/s" "CPU" "RSS"
printf "%s\n" "----------------------------------------------------------------------------------------------"
for vus in "${LEVELS[@]}"; do
  f="$OUT/v$vus.log"
  lat=$(grep -oE 'HTTP p50 / p95 / p99 \(ms\) \.+ [0-9.]+ / [0-9.]+ / [0-9.na/]+' "$f" 2>/dev/null | awk '{print $(NF-4), $(NF-2), $NF}')
  read -r p50 p95 p99 <<<"${lat:-- - -}"
  read -r cpu rss <<<"$(LC_ALL=C awk '
      $1 == "oxide-loadtest-showcase-1" { c[++n]=$2; if ($3>m) m=$3 }
      END { if (n==0) { print "- -"; exit } s=0; for (i=1;i<=n;i++) s+=c[i]
            printf "%.2f %.0f\n", s/n/100, m }' "$OUT/v$vus/containers.txt" 2>/dev/null || echo "- -")"
  printf "%-6s %-11s %-8s %-8s %-8s %-8s %-9s %-10s %-9s %s MB\n" \
    "$vus" \
    "$(num "$f" 'HTTP requests/s \.+ [0-9.]+')" \
    "${p50:--}" "${p95:--}" "${p99:--}" \
    "$(num "$f" 'HTTP max latency \(ms\) \.+ [0-9.]+')" \
    "$(grep -oE 'HTTP failure rate \.+ [0-9.]+%' "$f" 2>/dev/null | tail -1 | grep -oE '[0-9.]+%' || echo '-')" \
    "$(num "$f" 'consumed during load \.+ ~[0-9]+')" \
    "${cpu} c" "${rss}"
done

echo
echo "The knee is where req/s stops rising while p95 keeps climbing. Levels past"
echo "it are the load generator queueing, not the service doing more work."
echo "logs: $OUT"
