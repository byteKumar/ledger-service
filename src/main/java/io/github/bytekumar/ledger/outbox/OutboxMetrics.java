package io.github.bytekumar.ledger.outbox;

import org.springframework.stereotype.Component;

import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;

/**
 * Outbox lag, read from the database at scrape time, so it is correct in every process
 * (including an API process that doesn't run the relay). If the query fails, Micrometer
 * reports NaN instead of failing the scrape.
 */
@Component
public class OutboxMetrics {

    public OutboxMetrics(OutboxRepository outbox, MeterRegistry registry) {
        Gauge.builder("ledger.outbox.pending", outbox, OutboxRepository::countUnpublished)
                .description("Committed events not yet published to Kafka")
                .register(registry);
        Gauge.builder("ledger.outbox.oldest.age", outbox, OutboxRepository::oldestUnpublishedAgeSeconds)
                .description("Age of the oldest unpublished event")
                .baseUnit("seconds")
                .register(registry);
    }
}
