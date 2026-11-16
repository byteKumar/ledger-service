package io.github.bytekumar.ledger.admin;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;

import io.github.bytekumar.ledger.IntegrationTest;
import io.github.bytekumar.ledger.account.AccountService;
import io.github.bytekumar.ledger.transfer.TransferCommand;
import io.github.bytekumar.ledger.transfer.TransferService;

@SuppressWarnings({"rawtypes", "unchecked"})
class ReconcileTest extends IntegrationTest {

    @Autowired
    AccountService accounts;

    @Autowired
    TransferService transfers;

    @Autowired
    ReconcileService reconcile;

    @Test
    void balancedBooksReport200PerCurrency() {
        long usd1 = accounts.create("a", "USD").id();
        long usd2 = accounts.create("b", "USD").id();
        long eur1 = accounts.create("c", "EUR").id();
        long eur2 = accounts.create("d", "EUR").id();
        accounts.deposit(usd1, 1_000);
        accounts.deposit(eur1, 500);
        transfers.transfer(new TransferCommand(usd1, usd2, 300, "USD", "k1"));
        transfers.transfer(new TransferCommand(eur1, eur2, 200, "EUR", "k2"));

        ResponseEntity<Map> res = http.getForEntity("/admin/reconcile", Map.class);

        assertThat(res.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(res.getBody()).containsEntry("consistent", true).containsEntry("transferCount", 2)
                .containsEntry("unbalancedTransfers", 0).containsEntry("accountsNotMatchingEntries", 0);
        List<Map> currencies = (List<Map>) res.getBody().get("currencies");
        assertThat(currencies).extracting(c -> c.get("currency")).containsExactly("EUR", "USD");
        assertThat(currencies).allSatisfy(c -> assertThat(c.get("totalBalances")).isEqualTo(c.get("totalDeposits")));
    }

    @Test
    void tamperedBalanceIsReportedWith500() {
        long a = accounts.create("a", "USD").id();
        long b = accounts.create("b", "USD").id();
        accounts.deposit(a, 1_000);
        transfers.transfer(new TransferCommand(a, b, 100, "USD", "k"));

        // Money appears from nowhere: a balance changed without an entry or a deposit.
        jdbc.sql("UPDATE accounts SET balance = balance + 1 WHERE id = :id").param("id", b).update();

        ResponseEntity<Map> res = http.getForEntity("/admin/reconcile", Map.class);
        assertThat(res.getStatusCode()).isEqualTo(HttpStatus.INTERNAL_SERVER_ERROR);
        assertThat(res.getBody()).containsEntry("consistent", false).containsEntry("accountsNotMatchingEntries", 1);
        assertThat((List<Number>) res.getBody().get("accountIdsNotMatchingEntriesSample")).extracting(Number::longValue)
                .containsExactly(b);
    }

    @Test
    void halfWrittenTransferIsReported() {
        long a = accounts.create("a", "USD").id();
        long b = accounts.create("b", "USD").id();
        accounts.deposit(a, 1_000);
        long transferId = transfers.transfer(new TransferCommand(a, b, 100, "USD", "k")).transfer().id();

        // Lose the credit entry (and the money it represents), as a bug outside a transaction might.
        jdbc.sql("DELETE FROM entries WHERE transfer_id = :id AND amount > 0").param("id", transferId).update();
        jdbc.sql("UPDATE accounts SET balance = balance - 100 WHERE id = :id").param("id", b).update();

        ReconcileService.Report report = reconcile.reconcile();
        assertThat(report.consistent()).isFalse();
        assertThat(report.unbalancedTransferIdsSample()).containsExactly(transferId);
        assertThat(report.currencies()).singleElement().satisfies(c -> assertThat(c.balanced()).isFalse());
    }

    /**
     * Reconcile runs 30 times while 8 threads commit transfers; no run may raise a false
     * alarm. (This passes at READ COMMITTED too, because each check is a single statement;
     * see ReconcileService.)
     */
    @Test
    void reconcileDuringLiveTransfersNeverReportsFalseViolations() throws Exception {
        List<Long> ids = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            long id = accounts.create("acct" + i, "USD").id();
            accounts.deposit(id, 1_000_000);
            ids.add(id);
        }
        AtomicBoolean running = new AtomicBoolean(true);
        ExecutorService pool = Executors.newFixedThreadPool(8);
        List<Future<?>> writers = new ArrayList<>();
        for (int t = 0; t < 8; t++) {
            int thread = t;
            writers.add(pool.submit(() -> {
                Random rnd = new Random(thread);
                int n = 0;
                while (running.get()) {
                    long from = ids.get(rnd.nextInt(ids.size()));
                    long to = ids.get(rnd.nextInt(ids.size()));
                    if (from == to) {
                        continue;
                    }
                    try {
                        transfers.transfer(new TransferCommand(from, to, 1 + rnd.nextInt(100), "USD", "t" + thread + "-" + n++));
                    } catch (RuntimeException ignored) {
                        // gave up under contention: irrelevant here, the books still balance
                    }
                }
            }));
        }

        List<ReconcileService.Report> reports = new ArrayList<>();
        for (int i = 0; i < 30; i++) {
            reports.add(reconcile.reconcile());
        }
        running.set(false);
        for (Future<?> w : writers) {
            w.get(30, TimeUnit.SECONDS);
        }
        pool.shutdown();

        assertThat(reports).allSatisfy(r -> assertThat(r.consistent()).as("report %s", r).isTrue());
        assertThat(reports.get(reports.size() - 1).transferCount()).isGreaterThan(reports.get(0).transferCount());
    }
}
