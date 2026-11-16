package io.github.bytekumar.ledger.bench;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.simple.JdbcClient;

import com.fasterxml.jackson.databind.ObjectMapper;

import io.github.bytekumar.ledger.account.AccountService;
import io.github.bytekumar.ledger.admin.ReconcileService;
import io.github.bytekumar.ledger.transfer.ConcurrencyConflictException;
import io.github.bytekumar.ledger.transfer.TransferCommand;
import io.github.bytekumar.ledger.transfer.TransferRejectedException;
import io.github.bytekumar.ledger.transfer.TransferService;
import io.micrometer.core.instrument.MeterRegistry;

/**
 * Spec section 5 concurrency test: 100 accounts, 32 threads, 200,000 random transfers of
 * which ~5% resend an earlier request (same idempotency key and body), repeated 10 times.
 * Calls TransferService in-process against a real local Postgres (no HTTP) because this
 * measures correctness, not request latency.
 *
 * Not part of `mvn test`. Run with bench/run-concurrency.sh, which sets:
 *   -Dtest.excludedGroups= -Dgroups=bench -Dtest=ConcurrencyBenchmark
 *   -Dbench.runs=10 -Dbench.transfers=200000 -Dbench.out=bench/results/...
 *
 * Every run starts from empty tables. The outbox relay and consumer are off: events are
 * still written, but nothing publishes them, so Kafka plays no part in these numbers.
 */
@Tag("bench")
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.NONE, properties = {
        "spring.datasource.url=${BENCH_DB_URL:jdbc:postgresql://localhost:5432/ledger_bench}",
        "ledger.outbox.relay.enabled=false",
        "ledger.consumer.enabled=false",
        "spring.kafka.admin.auto-create=false",
        "logging.level.root=WARN"})
class ConcurrencyBenchmark {

    @Autowired AccountService accounts;
    @Autowired TransferService transfers;
    @Autowired ReconcileService reconcile;
    @Autowired JdbcClient jdbc;
    @Autowired MeterRegistry registry;
    @Autowired ObjectMapper json;

    @Value("${bench.runs:1}") int runs;
    @Value("${bench.transfers:200000}") int transferCount;
    @Value("${bench.accounts:100}") int accountCount;
    @Value("${bench.threads:32}") int threads;
    @Value("${bench.duplicate-rate:0.05}") double duplicateRate;
    @Value("${bench.starting-balance:1000000}") long startingBalance;
    @Value("${bench.max-amount:10000}") int maxAmount;
    @Value("${bench.out:bench/results/concurrency.jsonl}") String out;

    @Test
    void run() throws Exception {
        Path outFile = Path.of(out);
        Files.createDirectories(outFile.getParent());
        Files.deleteIfExists(outFile);
        System.out.printf("concurrency benchmark: mode=%s runs=%d transfers=%d accounts=%d threads=%d duplicates=%.0f%%%n",
                transfers.lockingMode(), runs, transferCount, accountCount, threads, duplicateRate * 100);
        for (int run = 1; run <= runs; run++) {
            Map<String, Object> result = runOnce(run);
            System.out.println("RESULT " + json.writeValueAsString(result));
            Files.writeString(outFile, json.writeValueAsString(result) + "\n", StandardOpenOption.CREATE, StandardOpenOption.APPEND);
        }
    }

    private Map<String, Object> runOnce(int run) throws InterruptedException, IOException {
        jdbc.sql("TRUNCATE accounts, deposits, transfers, entries, outbox_events, consumer_processed_events RESTART IDENTITY CASCADE").update();
        List<Long> ids = new ArrayList<>();
        for (int i = 0; i < accountCount; i++) {
            long id = accounts.create("bench" + i, "USD").id();
            accounts.deposit(id, startingBalance);
            ids.add(id);
        }

        // ~5% of requests resend one of the previous 64 requests unchanged, so many
        // duplicates are still in flight alongside their original (the hard case).
        Random rnd = new Random(run);
        List<TransferCommand> commands = new ArrayList<>(transferCount);
        int duplicatesSent = 0;
        for (int i = 0; i < transferCount; i++) {
            if (i > 0 && rnd.nextDouble() < duplicateRate) {
                commands.add(commands.get(Math.max(0, i - 1 - rnd.nextInt(Math.min(64, i)))));
                duplicatesSent++;
                continue;
            }
            long from = ids.get(rnd.nextInt(accountCount));
            long to;
            do {
                to = ids.get(rnd.nextInt(accountCount));
            } while (to == from);
            commands.add(new TransferCommand(from, to, 1 + rnd.nextInt(maxAmount), "USD", "r" + run + "-" + i));
        }

        double versionRetriesBefore = counter("version_conflict");
        double raceRetriesBefore = counter("idempotency_race");
        double pgBugRetriesBefore = counter("postgres_multixact_bug");
        AtomicLong created = new AtomicLong();
        AtomicLong replayed = new AtomicLong();
        AtomicLong insufficient = new AtomicLong();
        AtomicLong gaveUp = new AtomicLong();
        AtomicLong otherErrors = new AtomicLong();
        AtomicLong postgresMultixactErrors = new AtomicLong();
        AtomicLong createdAmount = new AtomicLong();
        AtomicLong keyCreatedTwice = new AtomicLong();
        Set<String> createdKeys = ConcurrentHashMap.newKeySet();

        ExecutorService pool = Executors.newFixedThreadPool(threads);
        long start = System.nanoTime();
        for (TransferCommand cmd : commands) {
            pool.execute(() -> {
                try {
                    TransferService.Outcome o = transfers.transfer(cmd);
                    if (o.replayed()) {
                        replayed.incrementAndGet();
                    } else {
                        created.incrementAndGet();
                        createdAmount.addAndGet(cmd.amount());
                        if (!createdKeys.add(cmd.idempotencyKey())) {
                            keyCreatedTwice.incrementAndGet(); // money moved twice for one key
                        }
                    }
                } catch (TransferRejectedException e) {
                    if (e.reason() == TransferRejectedException.Reason.INSUFFICIENT_FUNDS) {
                        insufficient.incrementAndGet();
                    } else {
                        otherErrors.incrementAndGet();
                    }
                } catch (ConcurrencyConflictException e) {
                    gaveUp.incrementAndGet();
                } catch (RuntimeException e) {
                    // Postgres 14 internal error (XX000) seen under this workload; see RESULTS.md.
                    if (String.valueOf(e.getMessage()).contains("new multixact has more than one updating member")) {
                        postgresMultixactErrors.incrementAndGet();
                    } else {
                        otherErrors.incrementAndGet();
                        System.err.println("unexpected: " + e);
                    }
                }
            });
        }
        pool.shutdown();
        if (!pool.awaitTermination(2, TimeUnit.HOURS)) {
            throw new IllegalStateException("run " + run + " did not finish");
        }
        double seconds = (System.nanoTime() - start) / 1e9;

        // Correctness, measured from the database rather than assumed.
        ReconcileService.Report report = reconcile.reconcile();
        long unbalancedCurrencies = report.currencies().stream().filter(c -> !c.balanced()).count();
        long negativeBalances = count("SELECT count(*) FROM accounts WHERE balance < 0");
        long transferRows = count("SELECT count(*) FROM transfers");
        long transferRowAmount = count("SELECT coalesce(sum(amount), 0) FROM transfers");
        // One hash join + GROUP BY. (A first version used a correlated subquery; with no index
        // on outbox_events.aggregate_id that was O(n^2) and ran for 14+ minutes.)
        long transfersWithoutExactlyOneEvent = count("SELECT count(*) FROM transfers t"
                + " LEFT JOIN (SELECT aggregate_id, count(*) AS n FROM outbox_events GROUP BY aggregate_id) o"
                + " ON o.aggregate_id = t.id WHERE coalesce(o.n, 0) <> 1");

        long invariantViolations = unbalancedCurrencies + report.unbalancedTransfers()
                + report.accountsNotMatchingEntries() + negativeBalances;
        long duplicateMovements = keyCreatedTwice.get()
                + Math.abs(transferRows - created.get())
                + (transferRowAmount == createdAmount.get() ? 0 : 1);

        Map<String, Object> r = new java.util.LinkedHashMap<>();
        r.put("run", run);
        r.put("mode", transfers.lockingMode().name().toLowerCase());
        r.put("transfersRequested", transferCount);
        r.put("duplicateRequestsSent", duplicatesSent);
        r.put("seconds", Math.round(seconds * 100) / 100.0);
        r.put("requestsPerSecond", Math.round(transferCount / seconds));
        r.put("created", created.get());
        r.put("replayed", replayed.get());
        r.put("insufficientFunds", insufficient.get());
        r.put("gaveUp", gaveUp.get());
        r.put("postgresMultixactErrors", postgresMultixactErrors.get());
        r.put("otherErrors", otherErrors.get());
        r.put("versionConflictRetries", Math.round(counter("version_conflict") - versionRetriesBefore));
        r.put("idempotencyRaceRetries", Math.round(counter("idempotency_race") - raceRetriesBefore));
        r.put("postgresMultixactRetries", Math.round(counter("postgres_multixact_bug") - pgBugRetriesBefore));
        r.put("invariantViolations", invariantViolations);
        r.put("duplicateMovements", duplicateMovements);
        r.put("sumBalancesEqualsSumDeposits", unbalancedCurrencies == 0);
        r.put("transfersWithoutExactlyOneOutboxEvent", transfersWithoutExactlyOneEvent);
        return r;
    }

    private long count(String sql) {
        return jdbc.sql(sql).query(Long.class).single();
    }

    private double counter(String reason) {
        var c = registry.find("ledger.transfer.retries").tag("reason", reason).counter();
        return c == null ? 0 : c.count();
    }
}
