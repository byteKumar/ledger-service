package io.github.bytekumar.ledger.transfer;

import org.springframework.http.HttpStatus;

/** A transfer that can't happen as asked. Nothing was written. */
public class TransferRejectedException extends RuntimeException {

    public enum Reason {
        INSUFFICIENT_FUNDS(HttpStatus.UNPROCESSABLE_ENTITY),
        CURRENCY_MISMATCH(HttpStatus.UNPROCESSABLE_ENTITY),
        SAME_ACCOUNT(HttpStatus.BAD_REQUEST),
        IDEMPOTENCY_KEY_REUSED(HttpStatus.CONFLICT);

        public final HttpStatus status;

        Reason(HttpStatus status) {
            this.status = status;
        }
    }

    private final Reason reason;

    public TransferRejectedException(Reason reason, String message) {
        super(message);
        this.reason = reason;
    }

    public Reason reason() {
        return reason;
    }
}
