# Shared helpers for the bench scripts. Source it; don't run it.
# Everything runs on this one machine: app (jar), Postgres 14 (Homebrew), Kafka 4 (Homebrew), k6.

BENCH_DB="${BENCH_DB:-ledger_bench}"
JAR="target/ledger-service-0.1.0.jar"
APP_PORT=8080
KAFKA_BIN=/opt/homebrew/opt/kafka/bin

check_no_other_load() {
  if pgrep -fl 'k6 run|node bench|gatling|wrk |locust' | grep -v $$; then
    echo "Another load generator is running; refusing to benchmark (numbers would be skewed)." >&2
    exit 1
  fi
}

reset_db() {
  psql -h localhost -d "$BENCH_DB" -q -c \
    "TRUNCATE accounts, deposits, transfers, entries, outbox_events, consumer_processed_events RESTART IDENTITY CASCADE" 2>/dev/null || true
}

ensure_kafka() {
  if ! nc -z localhost 9092 2>/dev/null; then
    brew services run kafka >/dev/null
    for _ in $(seq 1 30); do nc -z localhost 9092 2>/dev/null && return 0; sleep 1; done
    echo "Kafka did not start" >&2; exit 1
  fi
}

# start_app <logfile> [extra spring args...]  -> sets APP_PID
start_app() {
  local log="$1"; shift
  java -jar "$JAR" --spring.datasource.url="jdbc:postgresql://localhost:5432/$BENCH_DB" "$@" > "$log" 2>&1 &
  APP_PID=$!
  for _ in $(seq 1 60); do
    curl -sf "localhost:$APP_PORT/actuator/health" >/dev/null 2>&1 && return 0
    sleep 1
  done
  echo "app did not start; see $log" >&2; exit 1
}

stop_app() {
  if [ -n "${APP_PID:-}" ]; then
    kill "$APP_PID" 2>/dev/null || true
    wait "$APP_PID" 2>/dev/null || true
    APP_PID=""
  fi
}

# warmup: 10 s at 100 req/s against 100 throwaway accounts, not recorded. Gives the JIT
# compiler a chance to compile the hot path so the first measured seconds aren't
# interpreter-speed. Disclosed in RESULTS.md.
warmup() {
  k6 run -q -e RATE=100 -e DURATION=10s -e ACCOUNTS=100 -e RUN_ID="warm-$(date +%s)" bench/k6/transfers.js > /dev/null 2>&1 || true
}
