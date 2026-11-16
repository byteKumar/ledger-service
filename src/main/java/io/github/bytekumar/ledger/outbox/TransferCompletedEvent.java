package io.github.bytekumar.ledger.outbox;

import java.time.OffsetDateTime;
import java.util.UUID;

/** JSON body of a TransferCompleted message on the ledger.transfers topic. */
public record TransferCompletedEvent(UUID eventId, long transferId, long fromAccountId, long toAccountId, long amount,
                                     String currency, OffsetDateTime occurredAt) {

    public static final String TYPE = "TransferCompleted";
}
