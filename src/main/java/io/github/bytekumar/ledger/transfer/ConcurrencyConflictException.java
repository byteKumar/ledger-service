package io.github.bytekumar.ledger.transfer;

/** Gave up after the configured number of attempts. Safe for the client to retry with the same key. */
public class ConcurrencyConflictException extends RuntimeException {

    public ConcurrencyConflictException(int attempts) {
        super("Transfer kept conflicting with concurrent updates after " + attempts + " attempts; retry with the same idempotency key");
    }
}
