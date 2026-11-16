package io.github.bytekumar.ledger.account;

import java.time.OffsetDateTime;

/** One row of the accounts table. Amounts are minor units (cents). */
public record Account(long id, String owner, String currency, long balance, long version, OffsetDateTime createdAt) {
}
