CREATE TABLE transfers (
    id               BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    -- The client's retry token. The UNIQUE index is what makes a transfer
    -- happen at most once per key, even when two retries race each other.
    idempotency_key  TEXT        NOT NULL,
    from_account_id  BIGINT      NOT NULL REFERENCES accounts (id),
    to_account_id    BIGINT      NOT NULL REFERENCES accounts (id),
    amount           BIGINT      NOT NULL CHECK (amount > 0),
    currency         CHAR(3)     NOT NULL,
    created_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT transfers_idempotency_key_uq UNIQUE (idempotency_key),
    CONSTRAINT transfers_distinct_accounts CHECK (from_account_id <> to_account_id)
);

ALTER TABLE entries ADD COLUMN transfer_id BIGINT REFERENCES transfers (id);

-- Every entry comes from exactly one source: a deposit or a transfer.
ALTER TABLE entries ADD CONSTRAINT entries_exactly_one_source
    CHECK ((deposit_id IS NULL) <> (transfer_id IS NULL));

CREATE INDEX entries_transfer_id_idx ON entries (transfer_id);
