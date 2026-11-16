package io.github.bytekumar.ledger.transfer;

import java.time.OffsetDateTime;

public record Transfer(long id, String idempotencyKey, long fromAccountId, long toAccountId, long amount,
                       String currency, OffsetDateTime createdAt) {

    /** True if this stored transfer is what the given request asked for. */
    boolean matches(TransferCommand cmd) {
        return fromAccountId == cmd.from() && toAccountId == cmd.to() && amount == cmd.amount()
                && currency.equals(cmd.currency());
    }
}
