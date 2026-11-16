-- Money is stored as BIGINT minor units (cents for USD), never as floating point.

CREATE TABLE accounts (
    id          BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    owner       TEXT        NOT NULL,
    currency    CHAR(3)     NOT NULL,
    balance     BIGINT      NOT NULL DEFAULT 0,
    -- Bumped on every balance change; optimistic locking compares it.
    version     BIGINT      NOT NULL DEFAULT 0,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    -- Last line of defence: even buggy code can't overdraw an account.
    CONSTRAINT accounts_balance_non_negative CHECK (balance >= 0)
);

-- Money entering the system from outside. The reconcile check compares
-- SUM(accounts.balance) with SUM(deposits.amount).
CREATE TABLE deposits (
    id          BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    account_id  BIGINT      NOT NULL REFERENCES accounts (id),
    amount      BIGINT      NOT NULL CHECK (amount > 0),
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Append-only history of every balance change. Signed amount:
-- positive = credit (money in), negative = debit (money out).
CREATE TABLE entries (
    id            BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    account_id    BIGINT      NOT NULL REFERENCES accounts (id),
    amount        BIGINT      NOT NULL CHECK (amount <> 0),
    balance_after BIGINT      NOT NULL,
    deposit_id    BIGINT      REFERENCES deposits (id),
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Serves "list entries for an account, newest first" with keyset pagination.
CREATE INDEX entries_account_id_id_idx ON entries (account_id, id DESC);
