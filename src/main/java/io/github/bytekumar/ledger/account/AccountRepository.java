package io.github.bytekumar.ledger.account;

import java.util.List;
import java.util.Optional;

import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Repository;

/** Plain SQL over JdbcClient, so every query (and later every lock) is visible. */
@Repository
public class AccountRepository {

    private static final String ACCOUNT_COLUMNS = "id, owner, currency, balance, version, created_at";
    private static final String ENTRY_COLUMNS = "id, account_id, amount, balance_after, deposit_id, created_at";

    private final JdbcClient jdbc;

    public AccountRepository(JdbcClient jdbc) {
        this.jdbc = jdbc;
    }

    public Account insert(String owner, String currency) {
        return jdbc.sql("INSERT INTO accounts (owner, currency) VALUES (:owner, :currency) RETURNING " + ACCOUNT_COLUMNS)
                .param("owner", owner)
                .param("currency", currency)
                .query(Account.class)
                .single();
    }

    public Optional<Account> findById(long id) {
        return jdbc.sql("SELECT " + ACCOUNT_COLUMNS + " FROM accounts WHERE id = :id")
                .param("id", id)
                .query(Account.class)
                .optional();
    }

    /**
     * Adds to the balance in one atomic statement and returns the new balance.
     * Postgres row-locks the account until the transaction ends, and the version bump
     * makes any concurrent optimistic transfer on this account retry.
     */
    public long addToBalance(long accountId, long amount) {
        return jdbc.sql("UPDATE accounts SET balance = balance + :amount, version = version + 1 WHERE id = :id RETURNING balance")
                .param("amount", amount)
                .param("id", accountId)
                .query(Long.class)
                .single();
    }

    public long insertDeposit(long accountId, long amount) {
        return jdbc.sql("INSERT INTO deposits (account_id, amount) VALUES (:accountId, :amount) RETURNING id")
                .param("accountId", accountId)
                .param("amount", amount)
                .query(Long.class)
                .single();
    }

    public void insertDepositEntry(long accountId, long amount, long balanceAfter, long depositId) {
        jdbc.sql("INSERT INTO entries (account_id, amount, balance_after, deposit_id) VALUES (:accountId, :amount, :balanceAfter, :depositId)")
                .param("accountId", accountId)
                .param("amount", amount)
                .param("balanceAfter", balanceAfter)
                .param("depositId", depositId)
                .update();
    }

    /**
     * Keyset pagination, newest first: "entries older than beforeId". Unlike OFFSET,
     * the cost doesn't grow with page number and rows inserted meanwhile don't shift pages.
     */
    public List<Entry> findEntries(long accountId, Long beforeId, int limit) {
        return jdbc.sql("SELECT " + ENTRY_COLUMNS + " FROM entries"
                        + " WHERE account_id = :accountId AND (CAST(:beforeId AS BIGINT) IS NULL OR id < :beforeId)"
                        + " ORDER BY id DESC LIMIT :limit")
                .param("accountId", accountId)
                .param("beforeId", beforeId)
                .param("limit", limit)
                .query(Entry.class)
                .list();
    }
}
