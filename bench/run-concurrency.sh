#!/usr/bin/env bash
# Spec section 5 concurrency test against local Postgres (database ledger_bench must exist).
# Usage: bench/run-concurrency.sh [runs] [transfers] [optimistic|pessimistic]
set -euo pipefail
cd "$(dirname "$0")/.."
source scripts/env.sh

RUNS="${1:-10}"
TRANSFERS="${2:-200000}"
MODE="${3:-optimistic}"
OUT="bench/results/concurrency-${MODE}.jsonl"

mvn -q -B test \
  -Dtest.excludedGroups= -Dgroups=bench -Dtest=ConcurrencyBenchmark -Dsurefire.failIfNoSpecifiedTests=false \
  -Dbench.runs="$RUNS" -Dbench.transfers="$TRANSFERS" -Dbench.out="$OUT" \
  -Dledger.transfer.locking="$MODE" \
  | grep -E '^(concurrency benchmark|RESULT|unexpected)' || true

echo "results: $OUT"
