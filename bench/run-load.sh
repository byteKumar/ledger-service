#!/usr/bin/env bash
# k6 load test: one 60 s step per rate, fresh app + empty tables for every step.
# Usage: bench/run-load.sh <optimistic|pessimistic> <accounts> <label> <rate> [rate...]
#   e.g. bench/run-load.sh optimistic 1000 load 250 500 1000 2000
set -euo pipefail
cd "$(dirname "$0")/.."
source scripts/env.sh
source bench/lib.sh

MODE="$1"; ACCOUNTS="$2"; LABEL="$3"; shift 3
OUT_DIR="bench/results/$LABEL"
mkdir -p "$OUT_DIR" bench/logs
trap stop_app EXIT

check_no_other_load
ensure_kafka
[ -f "$JAR" ] || mvn -q -B -DskipTests package

for RATE in "$@"; do
  reset_db
  start_app "bench/logs/app-$LABEL-$MODE-$ACCOUNTS-$RATE.log" --ledger.transfer.locking="$MODE"
  warmup
  reset_db
  echo "step rate=$RATE mode=$MODE accounts=$ACCOUNTS $(uptime | sed 's/.*load/load/')"
  k6 run -q -e RATE="$RATE" -e DURATION=60s -e ACCOUNTS="$ACCOUNTS" -e RUN_ID="$LABEL-$MODE-$RATE-$(date +%s)" \
    -e SUMMARY_OUT="$OUT_DIR/$MODE-a$ACCOUNTS-r$RATE.json" bench/k6/transfers.js | grep LOADRESULT || true
  # The books must still balance after every step.
  echo "reconcile: $(curl -s -o /dev/null -w '%{http_code}' localhost:$APP_PORT/admin/reconcile)" \
    "retries: $(curl -s localhost:$APP_PORT/actuator/prometheus | grep '^ledger_transfer_retries_total' | tr '\n' ' ')"
  stop_app
done
