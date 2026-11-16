package io.github.bytekumar.ledger.transfer;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

import io.github.bytekumar.ledger.IntegrationTest;
import io.github.bytekumar.ledger.account.AccountService;
import io.micrometer.core.instrument.MeterRegistry;

/**
 * Hammers TransferService from many threads against real Postgres and then checks the
 * money: nothing created or destroyed, no transfer applied twice. A smaller version of
 * the section 5 concurrency benchmark, sized to run in the normal test suite.
 */
class TransferConcurrencyTest extends IntegrationTest {

    private static final int THREADS = 32;

    @Autowired
    TransferService transfers;

    @Autowired
    AccountService accounts;

    @Autowired
    MeterRegistry registry;

    @Test
    void racingDuplicatesOfOneKeyMoveMoneyOnce() throws Exception {
        long a = accounts.create("a", "USD").id();
        long b = accounts.create("b", "USD").id();
        accounts.deposit(a, 1_000);
        TransferCommand cmd = new TransferCommand(a, b, 100, "USD", "racing-key");

        CountDownLatch startGate = new CountDownLatch(1);
        ExecutorService pool = Executors.newFixedThreadPool(THREADS);
        List<Future<TransferService.Outcome>> futures = new ArrayList<>();
        for (int i = 0; i < THREADS; i++) {
            futures.add(pool.submit(() -> {
                startGate.await();
                return transfers.transfer(cmd);
            }));
        }
        startGate.countDown();
        Set<Long> ids = new HashSet<>();
        int fresh = 0;
        for (Future<TransferService.Outcome> f : futures) {
            TransferService.Outcome out = f.get(30, TimeUnit.SECONDS);
            ids.add(out.transfer().id());
            if (!out.replayed()) {
                fresh++;
            }
        }
        pool.shutdown();

        assertThat(fresh).isEqualTo(1);
        assertThat(ids).hasSize(1);
        assertThat(accounts.get(a).balance()).isEqualTo(900);
        assertThat(accounts.get(b).balance()).isEqualTo(100);
    }

    @Test
    void randomTransfersWithDuplicatesKeepTheBooksBalanced() throws Exception {
        int accountCount = 10;   // few accounts = high contention = many version conflicts
        int transferCount = 4_000;
        long startingBalance = 100_000;
        List<Long> ids = new ArrayList<>();
        for (int i = 0; i < accountCount; i++) {
            long id = accounts.create("acct" + i, "USD").id();
            accounts.deposit(id, startingBalance);
            ids.add(id);
        }

        // ~5% of requests resend an earlier request unchanged, like a client retrying after a timeout.
        Random rnd = new Random(42);
        List<TransferCommand> commands = new ArrayList<>();
        for (int i = 0; i < transferCount; i++) {
            if (i > 0 && rnd.nextDouble() < 0.05) {
                commands.add(commands.get(rnd.nextInt(commands.size())));
                continue;
            }
            long from = ids.get(rnd.nextInt(accountCount));
            long to;
            do {
                to = ids.get(rnd.nextInt(accountCount));
            } while (to == from);
            commands.add(new TransferCommand(from, to, 1 + rnd.nextInt(5_000), "USD", "key-" + i));
        }
        double retriesBefore = retries();

        AtomicInteger created = new AtomicInteger();
        AtomicInteger replayed = new AtomicInteger();
        AtomicInteger insufficient = new AtomicInteger();
        AtomicInteger gaveUp = new AtomicInteger();
        Set<Long> createdIds = ConcurrentHashMap.newKeySet();
        ExecutorService pool = Executors.newFixedThreadPool(THREADS);
        List<Future<?>> futures = new ArrayList<>();
        for (TransferCommand cmd : commands) {
            futures.add(pool.submit(() -> {
                try {
                    TransferService.Outcome out = transfers.transfer(cmd);
                    if (out.replayed()) {
                        replayed.incrementAndGet();
                    } else {
                        created.incrementAndGet();
                        assertThat(createdIds.add(out.transfer().id())).as("transfer id created twice").isTrue();
                    }
                } catch (TransferRejectedException e) {
                    assertThat(e.reason()).isEqualTo(TransferRejectedException.Reason.INSUFFICIENT_FUNDS);
                    insufficient.incrementAndGet();
                } catch (ConcurrencyConflictException e) {
                    gaveUp.incrementAndGet();
                }
            }));
        }
        for (Future<?> f : futures) {
            f.get(5, TimeUnit.MINUTES);
        }
        pool.shutdown();

        long sumBalances = jdbc.sql("SELECT sum(balance) FROM accounts").query(Long.class).single();
        long sumDeposits = jdbc.sql("SELECT sum(amount) FROM deposits").query(Long.class).single();
        long transferRows = jdbc.sql("SELECT count(*) FROM transfers").query(Long.class).single();
        long unbalancedTransfers = jdbc.sql("SELECT count(*) FROM (SELECT transfer_id FROM entries WHERE transfer_id IS NOT NULL"
                + " GROUP BY transfer_id HAVING count(*) <> 2 OR sum(amount) <> 0) t").query(Long.class).single();
        long accountsNotMatchingEntries = jdbc.sql("SELECT count(*) FROM accounts a WHERE a.balance <>"
                + " (SELECT coalesce(sum(e.amount), 0) FROM entries e WHERE e.account_id = a.id)").query(Long.class).single();

        System.out.printf("concurrency test: created=%d replayed=%d insufficient=%d gaveUp=%d retries=%.0f%n",
                created.get(), replayed.get(), insufficient.get(), gaveUp.get(), retries() - retriesBefore);

        assertThat(created.get() + replayed.get() + insufficient.get() + gaveUp.get()).isEqualTo(transferCount);
        assertThat(sumBalances).isEqualTo(sumDeposits);
        assertThat(transferRows).isEqualTo(created.get());
        assertThat(unbalancedTransfers).isZero();
        assertThat(accountsNotMatchingEntries).isZero();
    }

    private double retries() {
        return registry.find("ledger.transfer.retries").counters().stream().mapToDouble(c -> c.count()).sum();
    }
}
