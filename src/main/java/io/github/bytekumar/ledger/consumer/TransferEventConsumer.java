package io.github.bytekumar.ledger.consumer;

import java.util.UUID;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import io.github.bytekumar.ledger.outbox.KafkaTopics;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;

/**
 * Example downstream consumer, showing how to consume an at-least-once stream safely.
 *
 * Kafka may deliver the same event more than once (relay re-sent it, or the consumer
 * crashed before committing its offset). Recording the event id and doing the real work
 * in ONE database transaction makes processing idempotent: a second delivery hits the
 * primary key and is skipped. The offset is committed only after this method returns.
 */
@Component
@ConditionalOnProperty(name = "ledger.consumer.enabled", havingValue = "true", matchIfMissing = true)
public class TransferEventConsumer {

    private final JdbcClient jdbc;
    private final ObjectMapper mapper;
    private final Counter processed;
    private final Counter duplicates;

    public TransferEventConsumer(JdbcClient jdbc, ObjectMapper mapper, MeterRegistry registry) {
        this.jdbc = jdbc;
        this.mapper = mapper;
        this.processed = Counter.builder("ledger.consumer.events").tag("result", "processed").register(registry);
        this.duplicates = Counter.builder("ledger.consumer.events").tag("result", "duplicate").register(registry);
    }

    @KafkaListener(topics = KafkaTopics.TRANSFERS, groupId = "${ledger.consumer.group-id:ledger-example-consumer}")
    @Transactional
    public void onTransferCompleted(ConsumerRecord<String, String> record) throws JsonProcessingException {
        JsonNode event = mapper.readTree(record.value());
        UUID eventId = UUID.fromString(event.get("eventId").asText());
        long transferId = event.get("transferId").asLong();

        int deliveries = jdbc.sql("INSERT INTO consumer_processed_events (event_id, transfer_id) VALUES (:eventId, :transferId)"
                        + " ON CONFLICT (event_id) DO UPDATE SET deliveries = consumer_processed_events.deliveries + 1"
                        + " RETURNING deliveries")
                .param("eventId", eventId)
                .param("transferId", transferId)
                .query(Integer.class)
                .single();

        if (deliveries > 1) {
            duplicates.increment();
            return;
        }
        // First delivery. A real consumer would do its side effect here (send a receipt,
        // update a read model) inside this same transaction.
        processed.increment();
    }
}
