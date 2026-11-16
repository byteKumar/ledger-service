# Results

Every number here was measured for real on the machine below, once per configuration unless the table says otherwise. Nothing was picked as the best of several runs. Raw outputs are in [`bench/results/`](bench/results), and the scripts that produced them are in [`bench/`](bench).

## Machine and setup

| | |
|---|---|
| Machine | MacBook Air (Mac15,12), Apple M3, 8 cores (4P + 4E), 16 GB RAM, macOS 26.6.2. Fanless, so sustained load can throttle |
| Java | OpenJDK 21.0.13 (Homebrew), default JVM flags |
| Spring Boot | 3.5.16, HikariCP pool 20 |
| PostgreSQL | 14.17 (Homebrew), default config (`fsync` and `synchronous_commit` on) |
| Kafka | 4.4.0 (Homebrew, KRaft, single broker) |
| Load tool | k6 v2.4.0 |
| Date | 2026-11-15/16 |

Everything ran on the same laptop: app, Postgres, Kafka and k6 competed for the same 8 cores. Colima/Docker was stopped during benchmarks, and no other load generator was running (checked before each run). Ordinary desktop apps were running; idle load average was about 2. Before each load step, the scripts waited for the 1-minute load average to drop below 4.

## Summary (spec template)

```
MACHINE: chip=Apple M3 cores=8 RAM=16 GB, Postgres=14.17, Java=21.0.13
TESTS: 45 tests, all passing? yes
CONCURRENCY (10 runs x 200,000 transfers, 32 threads): invariant violations=0 duplicate movements=0 retries=797,589 version-conflict + 16,839 idempotency-race + 143 Postgres-bug (with the final code; before the Postgres-bug retry: 0 / 0 / 848,567 + 18,362, and 147 requests failed with a Postgres internal error)
LOAD: rate req/s | transfers/s | p95/p99 ms | error %
      250  | 250.0  | 8.33 / 20.14     | 0
      500  | 499.7  | 9.67 / 35.76     | 0
      750  | 748.8  | 2.05 / 26.77     | 0
      1000 | 999.3  | 7.56 / 51.07     | 0
      1500 | 1497.2 | 35.85 / 105.82   | 0
      2000 | 1985.9 | 89.86 / 312.92   | 0
      3000 | 2503.5 | 3941.02 / 6295.66 | 0
      4000 | 3171.4 | 3721.60 / 4463.96 | 0
      highest rate with p99 < 50 ms: 750 req/s (1000 req/s measured 51.07 ms)
LOCKING (low / high contention): optimistic tps,p99=999.8, 36.49 ms / 988.9, 120.65 ms ; pessimistic tps,p99=1000.0, 22.09 ms / 999.5, 8.17 ms
OUTBOX after relay kill: committed=30,000 published=30,000 consumer dedupes=0
ANYTHING THAT FAILED OR SURPRISED YOU: Postgres 14 internal error under optimistic contention (fixed with a narrow retry); 15 ms run-to-run p99 noise; pessimistic beat optimistic under high contention; 0 dedupes because the kill landed between relay batches. Details below.
REPO pushed? yes
```

## Tests

`mvn test` runs **45 tests, 0 failures** (run after the last code change, against Postgres 14 and Kafka 4.4 in Testcontainers). The benchmark harness is tagged `bench` and is excluded from that count.

**CI:** GitHub Actions ([`.github/workflows/ci.yml`](.github/workflows/ci.yml)) runs `mvn verify` on `ubuntu-latest` with Java 21 and the same Testcontainers Postgres 14 and Kafka 4.4 images on every push to `main` and on pull requests. The first run ([43928330494](https://github.com/byteKumar/ledger-service/actions/runs/43928330494), commit `8887c68`) passed: **45 tests, 0 failures**, job time 1 min 15 s. The only difference from local runs: the optimistic high-contention unit test gave up on 495 of 4,000 transfers on the runner (221–404 locally). That number is reported, not asserted, and the books balanced. The benchmarks were not run on CI; all benchmark numbers below are from the laptop.

## 1. Concurrency test

[`ConcurrencyBenchmark`](src/test/java/io/github/bytekumar/ledger/bench/ConcurrencyBenchmark.java), run with `bench/run-concurrency.sh 10 200000 optimistic`.

- **Setup:** 100 accounts, 32 threads, 200,000 transfers per run, 10 runs, optimistic mode.
- **Duplicates:** ~5% of requests (about 10,000 per run) resend one of the previous 64 requests unchanged, so many duplicates race their original.
- **No HTTP:** it calls the transfer service in-process against local Postgres, because this test measures correctness.
- **Clean start:** every run begins from truncated tables.
- **Outbox:** the relay is off, so events are written but not published.

Correctness is measured from the database after each run, not assumed:
- **Invariant violations:** currencies where sum(balances) ≠ sum(deposits), plus transfers without exactly one debit and one credit summing to 0, plus accounts whose balance ≠ sum of their entries, plus negative balances.
- **Duplicate movements:** a key that created a transfer twice, plus any mismatch between transfer rows (count and total amount) and the successes the threads saw.

| | before the Postgres-bug retry | final code (with the retry) |
|---|---|---|
| raw data | `concurrency-optimistic-no-pg-retry.jsonl` | `concurrency-optimistic-pg-retry.jsonl` |
| invariant violations (10 runs) | **0** | **0** |
| duplicate money movements | **0** | **0** |
| transfers without exactly one outbox event | 0 | 0 |
| duplicate requests replayed | 100,059 | 100,068 |
| version-conflict retries | 848,567 | 797,589 |
| idempotency-race retries | 18,362 | 16,839 |
| Postgres-bug retries | n/a | 143 |
| failed with Postgres internal error | **147** | **0** |
| gave up after 10 attempts (503) | 77 | 55 |
| insufficient funds | 34 | 33 |
| in-process requests/s per run | 5,199 – 8,176 | 4,212 – 6,851 |

The requests/s row isn't a performance claim. Throughput fell run after run in the first set (7,915 → 5,199) although every run starts from empty tables. The second set started with the load average at 6, after the test suite. `kernel_task` was at 21% CPU right afterwards, which on a fanless Mac usually means heat throttling, but `pmset` recorded no thermal warnings, so the cause is **not confirmed**.

## 2. Load test

[`bench/k6/transfers.js`](bench/k6/transfers.js), run with `bench/run-load.sh optimistic 1000 load <rates…>`.

- **Load shape:** k6 `constant-arrival-rate` at a fixed rate for 60 s per step, so a slow server shows up as latency instead of a lower request rate.
- **Accounts:** 1,000 accounts, each with $1M, so funds never run out. Random pairs, 1–100 cents, unique idempotency key per request.
- **Per step:** a fresh app process and empty tables, then a **10 s warm-up at 100 req/s that isn't recorded** (lets the JIT compile the hot path), then the 60 s measurement, then `/admin/reconcile`.
- **Counted:** latency and errors cover only the `POST /transfers` requests, not account setup.
- **Background components:** the outbox relay and consumer were running, with Kafka up.

| target req/s | requests | transfers/s | p50 ms | p95 ms | p99 ms | max ms | errors | k6 dropped |
|---|---|---|---|---|---|---|---|---|
| 250 | 15,000 | 250.0 | 2.04 | 8.33 | 20.14 | 93.03 | 0% | 0 |
| 500 | 29,984 | 499.7 | 1.47 | 9.67 | 35.76 | 188.91 | 0% | 16 |
| 750* | 44,929 | 748.8 | 1.04 | 2.05 | 26.77 | 310.92 | 0% | 72 |
| 1000 | 59,959 | 999.3 | 0.79 | 7.56 | 51.07 | 182.39 | 0% | 41 |
| 1500 | 89,833 | 1497.2 | 0.51 | 35.85 | 105.82 | 339.49 | 0% | 167 |
| 2000 | 119,152 | 1985.9 | 0.51 | 89.86 | 312.92 | 813.13 | 0% | 849 |
| 3000 | 150,211 | 2503.5 | 293.87 | 3941.02 | 6295.66 | 6695.84 | 0% | 29,790 |
| 4000 | 190,287 | 3171.4 | 1954.67 | 3721.60 | 4463.96 | 4780.65 | 0% | 49,714 |

\* The 750 step was **added after** the main ladder, once 1000 had come in just over 50 ms, to find where the line falls. 1000 was not rerun.

- **Highest tested rate with p99 < 50 ms: 750 req/s.** The 1000 step measured 51.07 ms here but 36.49 ms in the locking run below (same setup, different run). On this laptop p99 varies by about 15 ms between runs, so the true limit is "around 750–1000 req/s".
- **Saturation.** At 3000 and 4000 req/s the service handled about 2,500–3,170 transfers/s, and latency went to seconds. k6 couldn't start the remaining iterations ("dropped"), so those are requests that were never sent, not errors.
- **No errors at any rate:** nothing timed out and nothing failed, and reconcile returned 200 after every step.
- **"k6 dropped" at low rates** (up to 72) means k6 briefly ran out of preallocated virtual users. It's a client-side artefact: those requests weren't sent.

## 3. Optimistic vs pessimistic locking

[`bench/results/locking/`](bench/results/locking). Same script, 1000 req/s for 60 s, once per cell, run back to back in this order: optimistic-1000, pessimistic-1000, optimistic-10, pessimistic-10.

| contention | mode | transfers/s | p95 ms | p99 ms | errors | retries |
|---|---|---|---|---|---|---|
| low (1,000 accounts) | optimistic | 999.8 | 1.37 | 36.49 | 0% | 77 version conflicts |
| low (1,000 accounts) | pessimistic | 1000.0 | 1.32 | 22.09 | 0% | 0 |
| high (10 accounts) | optimistic | 988.9 | 1.70 | **120.65** | **0.128%** (503, gave up) | 5,878 version conflicts, 1 Postgres-bug |
| high (10 accounts) | pessimistic | 999.5 | 1.17 | **8.17** | 0% | 0 |

- **High contention:** optimistic locking keeps redoing transactions that lost a race (5,878 retries). Some requests use up all 10 attempts and get a 503, and the tail is long. Pessimistic makes transfers on the same account wait in line on the row lock instead, which here gave a much shorter tail and no errors.
- **Low contention:** both modes kept up at 1000 req/s. Pessimistic had the lower p99 in this run, but the difference (22 vs 36 ms) is within the run-to-run noise seen above.
- **Surprise:** pessimistic had a *lower* p99 with 10 accounts than with 1,000. That's one run each, so I'm reporting it, not explaining it. The fixed run order on a laptop that may heat up could also favour the earlier cells.

## 4. Outbox with the relay killed mid-run

[`bench/run-outbox-kill.sh 500 1000 20 35`](bench/run-outbox-kill.sh), with results in `bench/results/outbox-kill/`. Measured once.

- **Processes:** the API (relay off, consumer on), a separate relay process (no web server) and k6 at 500 req/s for 60 s.
- **Fresh state:** a new Kafka topic, so its message count is exactly what this test published.
- **Fault:** the relay got `kill -9` (no graceful shutdown) 20 s after transfers started and was restarted at 35 s.

| | count |
|---|---|
| transfers committed | 30,000 |
| outbox events written | 30,000 |
| events marked published | 30,000 |
| messages on the Kafka topic | 30,000 |
| distinct events processed by the consumer | 30,000 |
| committed transfers never consumed | **0** |
| consumer dedupes (re-deliveries ignored) | **0** |
| max backlog while the relay was down | 8,335 events |
| API during the outage | 500 req/s, 0% errors, p99 31.11 ms |

- **Every event was published and consumed**, and the books reconciled (200).
- **The API wasn't affected by Kafka/relay downtime**, and the backlog went from 8,335 to 20 within about 2 s of the restart, including the relay JVM's startup (`pending.csv`, sampled once per second).
- **Why 0 dedupes:** at 500 req/s the relay spends most of its time sleeping between 100 ms polls, and the kill landed while no batch was in flight, so nothing was acknowledged by Kafka without being marked published. The re-send-and-dedupe path is covered by `OutboxTest.resentEventIsDeduplicatedByConsumer` (deliveries = 2, one row), but **this benchmark didn't exercise it**. A kill during the post-restart drain (500-event batches) would be more likely to produce duplicates; that wasn't tested.

## What failed or surprised us

1. **Postgres 14.17 internal error under optimistic contention.** `ERROR: new multixact has more than one updating member` (SQLSTATE XX000) hit 147 of 2,000,000 requests in the first concurrency set.
   - **Cause:** a transfer updates an account and then rolls back after a lost version check. Meanwhile, foreign-key checks from other transfers hold KEY SHARE locks on that account row, and Postgres keeps the rolled-back updater in the row's lock record.
   - **Status upstream:** this matches a bug with a proposed upstream patch (pgsql-hackers, "Discard aborted updaters when expanding a multixact", Sept 2026) that isn't released.
   - **Effect:** no money was ever affected (the transaction rolls back).
   - **Fix:** retry exactly that error (same SQLSTATE and message, nothing else), counted as `ledger_transfer_retries_total{reason="postgres_multixact_bug"}`. With the retry: 0 failures, 143 retries.
2. **My first benchmark harness hung.** The "one outbox event per transfer" check was a correlated subquery on an unindexed column, which is O(n²), and it ran for 14+ minutes. Rewritten as a join plus `GROUP BY`. The service wasn't involved.
3. **Negative outbox publish delays** (found in milestone 5): the delay mixed the database clock (Colima VM) with the JVM clock, about 25 ms apart. Fixed by computing it entirely in SQL.
4. **Deadlock without lock ordering** (milestone 3 experiment): updating `from` then `to` instead of lower id first produced `deadlock detected` within seconds.
5. **Noisy laptop numbers:** the same 1000 req/s optimistic configuration measured p99 51.07 ms and 36.49 ms in two runs, and concurrency throughput fell run over run, probably from heat (not confirmed).

## Reproduce

```bash
source scripts/env.sh
createdb ledger_bench
brew services run kafka
bench/run-concurrency.sh 10 200000 optimistic
bench/run-load.sh optimistic 1000 load 250 500 750 1000 1500 2000 3000 4000
bench/run-load.sh optimistic 1000 locking 1000      # repeat for pessimistic and for 10 accounts
bench/run-outbox-kill.sh 500 1000 20 35
```
