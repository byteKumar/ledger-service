# ledger-service

A double-entry ledger service: the core of a payments or wallet system. Every transfer debits one account and credits another in a single Postgres transaction, the books always balance, and a client retry never moves money twice.

Java 21 · Spring Boot 3.5 · PostgreSQL 14 · Flyway · Kafka 4 · Micrometer/Prometheus · JUnit 5 + Testcontainers · k6

## What it does

- **Accounts:** create, read balance, list the entry history (cursor-paginated, newest first). Deposits bring money in from outside.
- **Transfers:** `POST /transfers {from, to, amount, currency, idempotencyKey}` writes one debit entry and one credit entry and updates both balances in one transaction.
  - **Idempotent:** the same key returns the original transfer (`200`, `Idempotent-Replayed: true`) and never moves money again. The same key with a different body returns `409`.
  - **Concurrency control:** optimistic locking (a `version` column, bounded retry with jittered backoff, `503` + `Retry-After` when it gives up) or pessimistic `SELECT … FOR UPDATE`, chosen with `ledger.transfer.locking`. Accounts are always locked or updated lowest id first, so transfers can't deadlock.
  - Insufficient funds and currency mismatches return `422`, and nothing is written.
- **Transactional outbox:** each transfer inserts a `TransferCompleted` event in the same transaction. A relay publishes events to Kafka (`ledger.transfers`) and then marks them sent. Delivery is at least once. The example consumer dedupes by event id.
- **Reconciliation:** `GET /admin/reconcile` checks, per currency, that the sum of balances equals the sum of deposits, that every transfer has exactly two entries summing to zero, and that every balance equals the sum of its entries. It returns `200` when the books balance and `500` when they don't.
- **Metrics** (`/actuator/prometheus`): transfer latency histogram (`ledger_transfer_latency_seconds`, by mode and outcome), retries (`ledger_transfer_retries_total`, by reason), outbox lag (`ledger_outbox_pending`, `ledger_outbox_oldest_age_seconds`, `ledger_outbox_publish_delay_seconds`) and consumer duplicates (`ledger_consumer_events_total{result="duplicate"}`).

Money is stored as `BIGINT` minor units (cents), never floating point. A `CHECK (balance >= 0)` constraint backs up the insufficient-funds check in the database.

## How a transfer works

```
POST /transfers
  └─ attempt (one DB transaction; retried on a lost optimistic race)
       1. key already used?           → replay the original (or 409 if the body differs)
       2. read both accounts, low id first   (FOR UPDATE in pessimistic mode)
       3. INSERT transfer ON CONFLICT (idempotency_key) DO NOTHING
                                       → a racing duplicate waits here, then replays
       4. currency + funds checks      → 422, everything rolls back
       5. UPDATE accounts … WHERE version = ?   (low id first; 0 rows → roll back, retry)
       6. INSERT 2 entries (−amount, +amount)
       7. INSERT outbox event
  relay (every 100 ms): lock unpublished rows (SKIP LOCKED) → send to Kafka → wait for acks → mark published
```

## Run it locally

Prerequisites: Java 21, Maven 3.9, PostgreSQL 14+, Kafka 4 (KRaft). Tests also need Docker (Docker Desktop or Colima) for Testcontainers. On macOS with Homebrew:

```bash
brew install openjdk@21 maven postgresql@14 kafka colima docker
brew services start postgresql@14
createdb ledger
brew services run kafka        # KRaft, localhost:9092; `brew services stop kafka` to stop
source scripts/env.sh          # pins Java 21 and points Testcontainers at Colima
```

Run the tests (they start their own Postgres 14 and Kafka 4.4 containers):

```bash
colima start
mvn test
```

Run the service (Flyway creates the schema on startup):

```bash
mvn -DskipTests package
java -jar target/ledger-service-0.1.0.jar
```

Try it:

```bash
curl -s -XPOST localhost:8080/accounts -H 'content-type: application/json' -d '{"owner":"alice","currency":"USD"}'
curl -s -XPOST localhost:8080/accounts -H 'content-type: application/json' -d '{"owner":"bob","currency":"USD"}'
curl -s -XPOST localhost:8080/accounts/1/deposits -H 'content-type: application/json' -d '{"amount":10000}'
curl -s -XPOST localhost:8080/transfers -H 'content-type: application/json' \
  -d '{"from":1,"to":2,"amount":2500,"currency":"USD","idempotencyKey":"order-42"}'
curl -s 'localhost:8080/accounts/2/entries?limit=20'
curl -s localhost:8080/admin/reconcile
curl -s localhost:8080/actuator/prometheus | grep '^ledger_'
```

### Configuration

| Property (env var) | Default | Meaning |
|---|---|---|
| `DB_URL`, `DB_USER`, `DB_PASSWORD`, `DB_POOL_SIZE` | `jdbc:postgresql://localhost:5432/ledger`, `$USER`, empty, `20` | Postgres connection |
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092` | Kafka |
| `ledger.transfer.locking` (`LEDGER_TRANSFER_LOCKING`) | `optimistic` | `optimistic` or `pessimistic`; use one mode per database |
| `ledger.transfer.max-attempts` | `10` | attempts before a `503` |
| `ledger.outbox.relay.enabled` | `true` | set `false` to run the relay as a separate process |
| `ledger.outbox.relay.poll-interval-ms`, `batch-size` | `100`, `500` | relay pacing |
| `ledger.consumer.enabled` | `true` | the example deduping consumer |

## Known limits

- `/admin/*` has no authentication; in production it would sit behind admin auth or an internal port.
- Published outbox rows are never deleted; a real deployment needs a cleanup job.
- A single deposit or transfer is capped at $10B in cents (`Money.MAX_AMOUNT`); there is no per-account balance cap.
- Reconciliation scans all entries; at large scale it would run incrementally or per partition.
- Running two instances in different locking modes against one database can deadlock on a shared idempotency key (Postgres aborts one).

## Results

Benchmarks (concurrency, k6 load, optimistic vs pessimistic, outbox under relay failure) are added in milestone 6; see `RESULTS.md`.
