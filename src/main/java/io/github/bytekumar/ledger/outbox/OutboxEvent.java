package io.github.bytekumar.ledger.outbox;

import java.time.OffsetDateTime;
import java.util.UUID;

public record OutboxEvent(long id, UUID eventId, String eventType, long aggregateId, String payload,
                          OffsetDateTime createdAt, OffsetDateTime publishedAt) {
}
