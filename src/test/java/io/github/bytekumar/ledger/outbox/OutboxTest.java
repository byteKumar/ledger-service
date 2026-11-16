package io.github.bytekumar.ledger.outbox;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import org.junit.jupiter.api.Test;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;

import io.github.bytekumar.ledger.IntegrationTest;

@SuppressWarnings({"rawtypes", "unchecked"})
class OutboxTest extends IntegrationTest {

    private static final Duration TIMEOUT = Duration.ofSeconds(60);

    private long account(long deposit) {
        Map acc = http.postForObject("/accounts", Map.of("owner", "o", "currency", "USD"), Map.class);
        long id = ((Number) acc.get("id")).longValue();
        if (deposit > 0) {
            http.postForEntity("/accounts/" + id + "/deposits", Map.of("amount", deposit), Map.class);
        }
        return id;
    }

    private ResponseEntity<Map> transfer(long from, long to, long amount, String key) {
        Map<String, Object> body = new HashMap<>();
        body.put("from", from);
        body.put("to", to);
        body.put("amount", amount);
        body.put("currency", "USD");
        body.put("idempotencyKey", key);
        return http.postForEntity("/transfers", body, Map.class);
    }

    private long eventsFor(long transferId) {
        return jdbc.sql("SELECT count(*) FROM outbox_events WHERE aggregate_id = :id").param("id", transferId)
                .query(Long.class).single();
    }

    private UUID eventIdFor(long transferId) {
        return jdbc.sql("SELECT event_id FROM outbox_events WHERE aggregate_id = :id").param("id", transferId)
                .query(UUID.class).single();
    }

    private Integer deliveries(UUID eventId) {
        return jdbc.sql("SELECT deliveries FROM consumer_processed_events WHERE event_id = :id").param("id", eventId)
                .query(Integer.class).optional().orElse(null);
    }

    @Test
    void eachCommittedTransferWritesExactlyOneEvent() {
        long a = account(1_000);
        long b = account(0);

        long transferId = ((Number) transfer(a, b, 100, "k1").getBody().get("id")).longValue();
        transfer(a, b, 100, "k1"); // replay: no new event

        assertThat(eventsFor(transferId)).isEqualTo(1);
        Map<String, Object> payload = jdbc.sql("SELECT event_type, payload->>'amount' AS amount, payload->>'fromAccountId' AS from_id"
                + " FROM outbox_events WHERE aggregate_id = :id").param("id", transferId).query().singleRow();
        assertThat(payload).containsEntry("event_type", "TransferCompleted").containsEntry("amount", "100")
                .containsEntry("from_id", Long.toString(a));
    }

    @Test
    void rejectedTransferWritesNoEvent() {
        long a = account(50);
        long b = account(0);

        assertThat(transfer(a, b, 100, "k").getStatusCode()).isEqualTo(HttpStatus.UNPROCESSABLE_ENTITY);

        assertThat(jdbc.sql("SELECT count(*) FROM outbox_events").query(Long.class).single()).isZero();
    }

    @Test
    void relayPublishesAndConsumerProcessesEventOnce() {
        long a = account(1_000);
        long b = account(0);
        long transferId = ((Number) transfer(a, b, 100, "k").getBody().get("id")).longValue();
        UUID eventId = eventIdFor(transferId);

        await().atMost(TIMEOUT).until(() -> deliveries(eventId) != null);

        assertThat(jdbc.sql("SELECT published_at IS NOT NULL FROM outbox_events WHERE event_id = :id").param("id", eventId)
                .query(Boolean.class).single()).isTrue();
        assertThat(deliveries(eventId)).isEqualTo(1);
    }

    @Test
    void resentEventIsDeduplicatedByConsumer() {
        long a = account(1_000);
        long b = account(0);
        long transferId = ((Number) transfer(a, b, 100, "k").getBody().get("id")).longValue();
        UUID eventId = eventIdFor(transferId);
        await().atMost(TIMEOUT).until(() -> deliveries(eventId) != null);

        // Simulate the relay crashing after Kafka acked but before it marked the row published.
        jdbc.sql("UPDATE outbox_events SET published_at = NULL WHERE event_id = :id").param("id", eventId).update();

        // Kafka delivered it twice; the consumer kept one row and counted the duplicate.
        await().atMost(TIMEOUT).until(() -> Integer.valueOf(2).equals(deliveries(eventId)));
        assertThat(jdbc.sql("SELECT count(*) FROM consumer_processed_events WHERE event_id = :id").param("id", eventId)
                .query(Long.class).single()).isEqualTo(1);
    }
}
