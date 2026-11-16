package io.github.bytekumar.ledger.account;

import java.time.OffsetDateTime;

/** One line of an account's history. amount > 0 is a credit, amount < 0 a debit. */
public record Entry(long id, long accountId, long amount, long balanceAfter, Long depositId, OffsetDateTime createdAt) {
}
