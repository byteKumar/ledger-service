package io.github.bytekumar.ledger.outbox;

import java.util.List;
import java.util.UUID;

import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Repository;

@Repository
public class OutboxRepository {

    private static final String COLUMNS = "id, event_id, event_type, aggregate_id, payload::text AS payload, created_at, published_at";

    private final JdbcClient jdbc;

    public OutboxRepository(JdbcClient jdbc) {
        this.jdbc = jdbc;
    }

    /** Must be called inside the transaction that made the change the event describes. */
    public void insert(UUID eventId, String eventType, long aggregateId, String payloadJson) {
        jdbc.sql("INSERT INTO outbox_events (event_id, event_type, aggregate_id, payload)"
                        + " VALUES (:eventId, :eventType, :aggregateId, CAST(:payload AS jsonb))")
                .param("eventId", eventId)
                .param("eventType", eventType)
                .param("aggregateId", aggregateId)
                .param("payload", payloadJson)
                .update();
    }

    /**
     * Oldest unpublished events, row-locked. SKIP LOCKED makes a second relay instance take
     * the next rows instead of waiting on (or re-sending) rows the first one is working on.
     */
    public List<OutboxEvent> lockUnpublishedBatch(int limit) {
        return jdbc.sql("SELECT " + COLUMNS + " FROM outbox_events WHERE published_at IS NULL"
                        + " ORDER BY id LIMIT :limit FOR UPDATE SKIP LOCKED")
                .param("limit", limit)
                .query(OutboxEvent.class)
                .list();
    }

    public void markPublished(List<Long> ids) {
        jdbc.sql("UPDATE outbox_events SET published_at = now() WHERE id IN (:ids)")
                .param("ids", ids)
                .update();
    }
}
