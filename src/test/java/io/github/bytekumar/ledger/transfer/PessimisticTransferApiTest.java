package io.github.bytekumar.ledger.transfer;

import org.springframework.test.context.TestPropertySource;

/** Runs every transfer API test again with SELECT ... FOR UPDATE locking: same behaviour, different mechanism. */
@TestPropertySource(properties = "ledger.transfer.locking=pessimistic")
class PessimisticTransferApiTest extends TransferApiTest {
}
