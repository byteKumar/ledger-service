package io.github.bytekumar.ledger.transfer;

/** A validated transfer request, independent of HTTP. */
public record TransferCommand(long from, long to, long amount, String currency, String idempotencyKey) {
}
