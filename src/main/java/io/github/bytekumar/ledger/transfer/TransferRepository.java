package io.github.bytekumar.ledger.transfer;

import java.util.Optional;

import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Repository;

@Repository
public class TransferRepository {

    private static final String COLUMNS = "id, idempotency_key, from_account_id, to_account_id, amount, currency, created_at";

    private final JdbcClient jdbc;

    public TransferRepository(JdbcClient jdbc) {
        this.jdbc = jdbc;
    }

    public Optional<Transfer> findById(long id) {
        return jdbc.sql("SELECT " + COLUMNS + " FROM transfers WHERE id = :id")
                .param("id", id)
                .query(Transfer.class)
                .optional();
    }

    public Optional<Transfer> findByKey(String idempotencyKey) {
        return jdbc.sql("SELECT " + COLUMNS + " FROM transfers WHERE idempotency_key = :key")
                .param("key", idempotencyKey)
                .query(Transfer.class)
                .optional();
    }

    /**
     * Claims the idempotency key. If another transaction holds the same key but hasn't
     * committed yet, Postgres makes this INSERT wait for it. If that one commits, we get
     * no row back (key taken); if it rolls back, our insert goes ahead.
     */
    public Optional<Transfer> insertIfKeyUnused(TransferCommand cmd) {
        return jdbc.sql("INSERT INTO transfers (idempotency_key, from_account_id, to_account_id, amount, currency)"
                        + " VALUES (:key, :from, :to, :amount, :currency)"
                        + " ON CONFLICT (idempotency_key) DO NOTHING RETURNING " + COLUMNS)
                .param("key", cmd.idempotencyKey())
                .param("from", cmd.from())
                .param("to", cmd.to())
                .param("amount", cmd.amount())
                .param("currency", cmd.currency())
                .query(Transfer.class)
                .optional();
    }
}
