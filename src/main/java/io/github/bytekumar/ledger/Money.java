package io.github.bytekumar.ledger;

public final class Money {

    /**
     * Largest single deposit or transfer, in minor units ($10 billion in cents). Keeps
     * request amounts far from Long.MAX_VALUE so one request can't overflow a BIGINT
     * balance (which Postgres would reject with a 500). Overflowing through ~9 million
     * maximum-size deposits into one account is still possible in theory; a per-account
     * balance limit would close that.
     */
    public static final long MAX_AMOUNT = 1_000_000_000_000L;

    private Money() {
    }
}
