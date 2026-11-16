package io.github.bytekumar.ledger.transfer;

import static org.assertj.core.api.Assertions.assertThat;

import java.sql.SQLException;

import org.junit.jupiter.api.Test;
import org.springframework.jdbc.UncategorizedSQLException;

/** The retry must catch exactly the known Postgres bug and nothing else. */
class PostgresTransientErrorsTest {

    private static UncategorizedSQLException wrapped(String message, String sqlState) {
        return new UncategorizedSQLException("UPDATE accounts", "UPDATE accounts SET ...", new SQLException(message, sqlState));
    }

    @Test
    void recognisesTheMultixactBugAsSpringWrapsIt() {
        assertThat(PostgresTransientErrors.isMultixactBug(wrapped(
                "ERROR: new multixact has more than one updating member: 0 3[10270517 (nokeyupd), 10270523 (keysh), 10270523 (nokeyupd)]",
                "XX000"))).isTrue();
    }

    @Test
    void doesNotRetryOtherInternalErrors() {
        assertThat(PostgresTransientErrors.isMultixactBug(wrapped("ERROR: could not read block 7 in file", "XX000"))).isFalse();
        assertThat(PostgresTransientErrors.isMultixactBug(wrapped("ERROR: XX001 data corrupted", "XX001"))).isFalse();
    }

    @Test
    void doesNotRetrySameTextUnderAnotherSqlState() {
        assertThat(PostgresTransientErrors.isMultixactBug(wrapped(PostgresTransientErrors.MULTIXACT_BUG, "23505"))).isFalse();
        assertThat(PostgresTransientErrors.isMultixactBug(new RuntimeException(PostgresTransientErrors.MULTIXACT_BUG))).isFalse();
    }
}
