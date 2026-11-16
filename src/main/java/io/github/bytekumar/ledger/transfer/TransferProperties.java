package io.github.bytekumar.ledger.transfer;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;

/**
 * ledger.transfer.* settings. The locking mode is per deployment: run every instance
 * against one database in the same mode (see TransferService for why).
 */
@ConfigurationProperties("ledger.transfer")
public record TransferProperties(
        @DefaultValue("10") int maxAttempts,
        @DefaultValue("optimistic") LockingMode locking) {
}
