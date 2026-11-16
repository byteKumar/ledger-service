package io.github.bytekumar.ledger.account;

import java.time.OffsetDateTime;

/**
 * One line of an account's history. amount > 0 is a credit, amount < 0 a debit.
 * Exactly one of depositId / transferId is set.
 */
public record Entry(long id, long accountId, long amount, long balanceAfter, Long depositId, Long transferId,
                    OffsetDateTime createdAt) {
}
