#!/usr/bin/env bash
# Outbox test from spec section 5: kill the relay mid-run, bring it back, and check that
# every committed transfer's event reaches Kafka at least once and that the consumer
# dedupes the re-sent ones.
#
# Three roles: API process (relay OFF, consumer ON), relay process (no web server, relay
# ON), k6. The relay gets kill -9 (no graceful shutdown) KILL_AT seconds into the load
# and is restarted at RESTART_AT.
# Usage: bench/run-outbox-kill.sh [rate] [accounts] [kill_at_s] [restart_at_s]
set -euo pipefail
cd "$(dirname "$0")/.."
source scripts/env.sh
source bench/lib.sh

RATE="${1:-300}"; ACCOUNTS="${2:-1000}"; KILL_AT="${3:-20}"; RESTART_AT="${4:-35}"
OUT_DIR=bench/results/outbox-kill; mkdir -p "$OUT_DIR" bench/logs
TOPIC=ledger.transfers
GROUP="bench-outbox-$(date +%s)"
RELAY_PID=""
cleanup() { stop_app; [ -n "$RELAY_PID" ] && kill "$RELAY_PID" 2>/dev/null || true; }
trap cleanup EXIT

check_no_other_load
ensure_kafka
[ -f "$JAR" ] || mvn -q -B -DskipTests package

# Fresh topic so its message count is exactly what this test published.
$KAFKA_BIN/kafka-topics --bootstrap-server localhost:9092 --delete --topic "$TOPIC" >/dev/null 2>&1 || true
sleep 3
$KAFKA_BIN/kafka-topics --bootstrap-server localhost:9092 --create --topic "$TOPIC" --partitions 3 --replication-factor 1 >/dev/null
reset_db

start_relay() {
  java -jar "$JAR" --spring.datasource.url="jdbc:postgresql://localhost:5432/$BENCH_DB" \
    --spring.main.web-application-type=none --ledger.consumer.enabled=false --ledger.outbox.relay.enabled=true \
    >> bench/logs/outbox-relay.log 2>&1 &
  RELAY_PID=$!
}
pending() { psql -h localhost -d "$BENCH_DB" -Atc "SELECT count(*) FROM outbox_events WHERE published_at IS NULL"; }

: > bench/logs/outbox-relay.log
start_app bench/logs/outbox-api.log --ledger.outbox.relay.enabled=false --ledger.consumer.group-id="$GROUP"
start_relay
sleep 8   # relay context up; consumer group joined (first join takes a few seconds)

T0=$(date +%s)
k6 run -q -e RATE="$RATE" -e DURATION=60s -e ACCOUNTS="$ACCOUNTS" -e RUN_ID="outbox-$T0" \
  -e SUMMARY_OUT="$OUT_DIR/k6-r$RATE.json" bench/k6/transfers.js > "$OUT_DIR/k6-stdout.txt" 2>&1 &
K6_PID=$!

# k6 setup (account creation) takes a few seconds; time the kill from when transfers start.
until [ "$(psql -h localhost -d "$BENCH_DB" -Atc 'SELECT count(*) FROM transfers')" -gt 0 ]; do sleep 0.2; done
LOAD_START=$(date +%s)
echo "t,pending" > "$OUT_DIR/pending.csv"
MAX_PENDING=0
KILLED=0; RESTARTED=0
while kill -0 "$K6_PID" 2>/dev/null; do
  T=$(( $(date +%s) - LOAD_START ))
  P=$(pending); echo "$T,$P" >> "$OUT_DIR/pending.csv"
  [ "$P" -gt "$MAX_PENDING" ] && MAX_PENDING=$P
  if [ "$KILLED" = 0 ] && [ "$T" -ge "$KILL_AT" ]; then
    kill -9 "$RELAY_PID"; KILLED=1; echo "t=${T}s relay killed (kill -9), pending=$P"
  fi
  if [ "$KILLED" = 1 ] && [ "$RESTARTED" = 0 ] && [ "$T" -ge "$RESTART_AT" ]; then
    start_relay; RESTARTED=1; echo "t=${T}s relay restarted, pending=$P"
  fi
  sleep 1
done
wait "$K6_PID" || true
grep LOADRESULT "$OUT_DIR/k6-stdout.txt" || true

# Let the relay drain the backlog and the consumer catch up (max 180 s).
COMMITTED=$(psql -h localhost -d "$BENCH_DB" -Atc "SELECT count(*) FROM transfers")
for _ in $(seq 1 180); do
  P=$(pending)
  C=$(psql -h localhost -d "$BENCH_DB" -Atc "SELECT count(*) FROM consumer_processed_events")
  [ "$P" = 0 ] && [ "$C" -ge "$COMMITTED" ] && break
  sleep 1
done
sleep 5  # let any last re-deliveries land

q() { psql -h localhost -d "$BENCH_DB" -Atc "$1"; }
TOPIC_MESSAGES=$($KAFKA_BIN/kafka-get-offsets --bootstrap-server localhost:9092 --topic "$TOPIC" --time -1 \
  | awk -F: '{s+=$3} END {print s}')
cat > "$OUT_DIR/result.json" <<EOF
{
  "rate": $RATE, "accounts": $ACCOUNTS, "killAtSeconds": $KILL_AT, "restartAtSeconds": $RESTART_AT,
  "transfersCommitted": $(q "SELECT count(*) FROM transfers"),
  "outboxEvents": $(q "SELECT count(*) FROM outbox_events"),
  "eventsMarkedPublished": $(q "SELECT count(*) FROM outbox_events WHERE published_at IS NOT NULL"),
  "eventsStillPending": $(pending),
  "maxPendingDuringRun": $MAX_PENDING,
  "messagesOnTopic": $TOPIC_MESSAGES,
  "consumerDistinctEvents": $(q "SELECT count(*) FROM consumer_processed_events"),
  "consumerDedupes": $(q "SELECT coalesce(sum(deliveries - 1), 0) FROM consumer_processed_events"),
  "committedTransfersNeverConsumed": $(q "SELECT count(*) FROM outbox_events o WHERE NOT EXISTS (SELECT 1 FROM consumer_processed_events c WHERE c.event_id = o.event_id)"),
  "reconcileHttpStatus": $(curl -s -o /dev/null -w '%{http_code}' localhost:$APP_PORT/admin/reconcile)
}
EOF
cat "$OUT_DIR/result.json"
