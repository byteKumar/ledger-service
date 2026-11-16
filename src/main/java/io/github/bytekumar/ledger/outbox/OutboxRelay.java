package io.github.bytekumar.ledger.outbox;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.support.TransactionTemplate;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;

/**
 * Publishes outbox rows to Kafka, oldest first, then marks them published.
 *
 * Delivery is at-least-once: if the process dies after Kafka acknowledged a batch but
 * before the UPDATE commits, the rows are still unpublished and get sent again on the next
 * run. Consumers therefore dedupe by event id. The opposite failure (marked published but
 * never sent) can't happen, because we only mark rows after Kafka acknowledged them.
 *
 * Runs in the API process by default; set ledger.outbox.relay.enabled=false there and run
 * a separate instance with it on to scale or kill the relay independently.
 */
@Component
@ConditionalOnProperty(name = "ledger.outbox.relay.enabled", havingValue = "true", matchIfMissing = true)
public class OutboxRelay {

    private static final Logger log = LoggerFactory.getLogger(OutboxRelay.class);

    private final OutboxRepository outbox;
    private final KafkaTemplate<String, String> kafka;
    private final TransactionTemplate tx;
    private final int batchSize;
    private final long sendTimeoutMs;
    private final Timer publishDelay;

    public OutboxRelay(OutboxRepository outbox, KafkaTemplate<String, String> kafka, TransactionTemplate tx,
                       @Value("${ledger.outbox.relay.batch-size:500}") int batchSize,
                       @Value("${ledger.outbox.relay.send-timeout-ms:15000}") long sendTimeoutMs,
                       MeterRegistry registry) {
        this.publishDelay = Timer.builder("ledger.outbox.publish.delay")
                .description("Time from the transfer committing its event to Kafka acknowledging it")
                .publishPercentileHistogram()
                .register(registry);
        this.outbox = outbox;
        this.kafka = kafka;
        this.tx = tx;
        this.batchSize = batchSize;
        this.sendTimeoutMs = sendTimeoutMs;
    }

    /** Drains the backlog in batches, then sleeps poll-interval-ms before looking again. */
    @Scheduled(fixedDelayString = "${ledger.outbox.relay.poll-interval-ms:100}")
    public void poll() {
        try {
            while (publishBatch() == batchSize) {
                // full batch: there may be more waiting
            }
        } catch (RuntimeException e) {
            log.warn("Outbox relay batch failed; the rows stay unpublished and will be retried: {}", e.toString());
        }
    }

    /** One transaction: lock a batch, send it, wait for every ack, mark it published. Returns the batch size. */
    public int publishBatch() {
        Integer sent = tx.execute(status -> {
            List<OutboxEvent> batch = outbox.lockUnpublishedBatch(batchSize);
            if (batch.isEmpty()) {
                return 0;
            }
            List<CompletableFuture<SendResult<String, String>>> acks = batch.stream().map(this::send).toList();
            for (CompletableFuture<SendResult<String, String>> ack : acks) {
                awaitAck(ack); // throws -> transaction rolls back -> whole batch retried later
            }
            List<Double> waitedSeconds = outbox.markPublished(batch.stream().map(OutboxEvent::id).toList());
            for (double seconds : waitedSeconds) {
                publishDelay.record(Duration.ofNanos((long) (seconds * 1e9)));
            }
            return batch.size();
        });
        return sent == null ? 0 : sent;
    }

    private CompletableFuture<SendResult<String, String>> send(OutboxEvent e) {
        ProducerRecord<String, String> record =
                new ProducerRecord<>(KafkaTopics.TRANSFERS, Long.toString(e.aggregateId()), e.payload());
        record.headers().add("event-id", e.eventId().toString().getBytes(StandardCharsets.UTF_8));
        record.headers().add("event-type", e.eventType().getBytes(StandardCharsets.UTF_8));
        return kafka.send(record);
    }

    private void awaitAck(CompletableFuture<SendResult<String, String>> ack) {
        try {
            ack.get(sendTimeoutMs, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted while waiting for Kafka", e);
        } catch (ExecutionException | TimeoutException e) {
            throw new IllegalStateException("Kafka send failed", e);
        }
    }
}
