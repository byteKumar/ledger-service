package io.github.bytekumar.ledger.transfer;

import java.sql.SQLException;

/**
 * Recognises database errors where the transaction was rolled back cleanly and trying again
 * is safe and likely to succeed.
 *
 * Only one is handled: a Postgres bug (seen on 14.17) that raises internal error XX000
 * "new multixact has more than one updating member" when a row's lock record still lists
 * an updater that has just rolled back. Optimistic transfers roll back a lot, and foreign
 * keys from transfers/entries take KEY SHARE locks on account rows, which is exactly that
 * mix. An upstream patch was proposed in Sept 2026; RESULTS.md has the details and counts.
 *
 * Deliberately narrow: other XX000 errors may mean real corruption and must not be retried.
 */
final class PostgresTransientErrors {

    static final String MULTIXACT_BUG = "new multixact has more than one updating member";

    private PostgresTransientErrors() {
    }

    static boolean isMultixactBug(Throwable error) {
        for (Throwable t = error; t != null; t = t.getCause()) {
            if (t instanceof SQLException sql && "XX000".equals(sql.getSQLState())
                    && sql.getMessage() != null && sql.getMessage().contains(MULTIXACT_BUG)) {
                return true;
            }
        }
        return false;
    }
}
