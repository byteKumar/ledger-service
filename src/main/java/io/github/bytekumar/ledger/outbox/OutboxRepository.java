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

    /** Backlog size: committed events not yet on Kafka. Uses the partial index. */
    public long countUnpublished() {
        return jdbc.sql("SELECT count(*) FROM outbox_events WHERE published_at IS NULL").query(Long.class).single();
    }

    /** How long the oldest unpublished event has been waiting; 0 when the backlog is empty. */
    public double oldestUnpublishedAgeSeconds() {
        return jdbc.sql("SELECT coalesce(extract(epoch FROM now() - min(created_at)), 0)::float8"
                        + " FROM outbox_events WHERE published_at IS NULL")
                .query(Double.class).single();
    }

    /**
     * Marks the rows published and returns, per row, how long it waited (in seconds).
     * Both timestamps come from the database clock: mixing the DB clock (created_at) with
     * the app's clock gave negative delays when the two machines' clocks differed.
     * clock_timestamp() is the real time now; now() would be when this transaction began,
     * before the Kafka sends, and would understate the delay.
     */
    public List<Double> markPublished(List<Long> ids) {
        return jdbc.sql("UPDATE outbox_events SET published_at = clock_timestamp() WHERE id IN (:ids)"
                        + " RETURNING extract(epoch FROM published_at - created_at)::float8")
                .param("ids", ids)
                .query(Double.class)
                .list();
    }
}
