package io.github.bytekumar.ledger.transfer;

import static org.assertj.core.api.Assertions.assertThat;

import org.springframework.test.context.TestPropertySource;

/** Runs every concurrency test again with SELECT ... FOR UPDATE locking. */
@TestPropertySource(properties = "ledger.transfer.locking=pessimistic")
class PessimisticTransferConcurrencyTest extends TransferConcurrencyTest {

    /** Rows are locked before they're read, so no version check can fail and nobody gives up. */
    @Override
    protected void assertModeSpecific(int gaveUp, double versionConflictRetries) {
        assertThat(transfers.lockingMode()).isEqualTo(LockingMode.PESSIMISTIC);
        assertThat(versionConflictRetries).isZero();
        assertThat(gaveUp).isZero();
    }
}
