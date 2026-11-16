package io.github.bytekumar.ledger.admin;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.time.Duration;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.actuate.observability.AutoConfigureObservability;

import io.github.bytekumar.ledger.IntegrationTest;
import io.github.bytekumar.ledger.account.AccountService;
import io.github.bytekumar.ledger.transfer.TransferCommand;
import io.github.bytekumar.ledger.transfer.TransferService;

/** Spring Boot switches metrics exporters off in tests unless asked; this test asks. */
@AutoConfigureObservability
class MetricsTest extends IntegrationTest {

    @Autowired
    AccountService accounts;

    @Autowired
    TransferService transfers;

    @Test
    void prometheusEndpointExposesLedgerMetrics() {
        long a = accounts.create("a", "USD").id();
        long b = accounts.create("b", "USD").id();
        accounts.deposit(a, 1_000);
        transfers.transfer(new TransferCommand(a, b, 100, "USD", "k"));
        // the relay records publish delay once it has sent the event
        await().atMost(Duration.ofSeconds(60)).until(() ->
                jdbc.sql("SELECT count(*) FROM outbox_events WHERE published_at IS NULL").query(Long.class).single() == 0);

        String body = http.getForObject("/actuator/prometheus", String.class);

        assertThat(body)
                .containsPattern("ledger_transfer_latency_seconds_bucket\\{[^}]*mode=\"optimistic\"[^}]*outcome=\"created\"")
                .containsPattern("ledger_transfer_retries_total\\{[^}]*reason=\"version_conflict\"")
                .contains("ledger_outbox_pending ")
                .contains("ledger_outbox_oldest_age_seconds ")
                .contains("ledger_outbox_publish_delay_seconds_count")
                .containsPattern("ledger_consumer_events_total\\{[^}]*result=\"duplicate\"");
    }
}
