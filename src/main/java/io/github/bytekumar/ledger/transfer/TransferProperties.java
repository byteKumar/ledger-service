package io.github.bytekumar.ledger.transfer;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;

/** ledger.transfer.* settings. */
@ConfigurationProperties("ledger.transfer")
public record TransferProperties(@DefaultValue("10") int maxAttempts) {
}
