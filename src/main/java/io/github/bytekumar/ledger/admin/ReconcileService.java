package io.github.bytekumar.ledger.admin;

import java.time.OffsetDateTime;
import java.util.List;

import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Isolation;
import org.springframework.transaction.annotation.Transactional;

/**
 * Checks that the books balance. Read-only; changes nothing.
 *
 * What keeps it free of false alarms while transfers are committing: each check is ONE SQL
 * statement, and Postgres runs one statement against one snapshot even at READ COMMITTED.
 * That's why balances and deposits are summed in the same statement. As two separate
 * queries, a deposit committing between them would make the totals disagree although
 * nothing is wrong. (Verified: the live-transfer test passes at READ COMMITTED too.)
 *
 * REPEATABLE READ on top makes the whole report describe one moment, so counts, sample
 * ids and transferCount agree with each other.
 */
@Service
public class ReconcileService {

    private static final int SAMPLE = 20;

    private final JdbcClient jdbc;

    public ReconcileService(JdbcClient jdbc) {
        this.jdbc = jdbc;
    }

    /** Per currency, because adding USD cents to EUR cents means nothing. */
    public record CurrencyTotals(String currency, long totalBalances, long totalDeposits) {
        public boolean balanced() {
            return totalBalances == totalDeposits;
        }
    }

    public record Report(
            boolean consistent,
            OffsetDateTime checkedAt,
            List<CurrencyTotals> currencies,
            long transferCount,
            long unbalancedTransfers,
            List<Long> unbalancedTransferIdsSample,
            long accountsNotMatchingEntries,
            List<Long> accountIdsNotMatchingEntriesSample) {
    }

    @Transactional(readOnly = true, isolation = Isolation.REPEATABLE_READ)
    public Report reconcile() {
        OffsetDateTime checkedAt = jdbc.sql("SELECT now()").query(OffsetDateTime.class).single();

        // 1. The spec's invariant: money only enters through deposits, and transfers only
        //    move it, so per currency SUM(balances) must equal SUM(deposits).
        List<CurrencyTotals> currencies = jdbc.sql("""
                WITH b AS (SELECT currency, sum(balance) AS balances FROM accounts GROUP BY currency),
                     d AS (SELECT a.currency, sum(d.amount) AS deposits
                           FROM deposits d JOIN accounts a ON a.id = d.account_id GROUP BY a.currency)
                SELECT b.currency, b.balances AS total_balances, coalesce(d.deposits, 0) AS total_deposits
                FROM b LEFT JOIN d USING (currency)
                ORDER BY b.currency""")
                .query(CurrencyTotals.class).list();

        // 2. Double entry: every transfer has exactly one debit and one credit summing to 0.
        String unbalanced = """
                SELECT t.id FROM transfers t LEFT JOIN entries e ON e.transfer_id = t.id
                GROUP BY t.id HAVING count(e.id) <> 2 OR coalesce(sum(e.amount), 0) <> 0""";
        long unbalancedCount = count(unbalanced);
        List<Long> unbalancedSample = sample(unbalanced);

        // 3. Each stored balance equals the sum of that account's entries (its history).
        String drifted = """
                SELECT a.id FROM accounts a
                LEFT JOIN (SELECT account_id, sum(amount) AS total FROM entries GROUP BY account_id) e
                       ON e.account_id = a.id
                WHERE a.balance <> coalesce(e.total, 0)""";
        long driftedCount = count(drifted);
        List<Long> driftedSample = sample(drifted);

        long transferCount = jdbc.sql("SELECT count(*) FROM transfers").query(Long.class).single();
        boolean consistent = currencies.stream().allMatch(CurrencyTotals::balanced)
                && unbalancedCount == 0 && driftedCount == 0;
        return new Report(consistent, checkedAt, currencies, transferCount, unbalancedCount, unbalancedSample,
                driftedCount, driftedSample);
    }

    private long count(String idQuery) {
        return jdbc.sql("SELECT count(*) FROM (" + idQuery + ") q").query(Long.class).single();
    }

    private List<Long> sample(String idQuery) {
        return jdbc.sql("SELECT id FROM (" + idQuery + ") q ORDER BY id LIMIT " + SAMPLE).query(Long.class).list();
    }
}
