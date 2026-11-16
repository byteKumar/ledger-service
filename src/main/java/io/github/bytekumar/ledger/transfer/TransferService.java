package io.github.bytekumar.ledger.transfer;

import java.util.Optional;
import java.util.concurrent.ThreadLocalRandom;

import org.springframework.stereotype.Service;
import org.springframework.transaction.support.TransactionTemplate;

import io.github.bytekumar.ledger.account.Account;
import io.github.bytekumar.ledger.account.AccountNotFoundException;
import io.github.bytekumar.ledger.account.AccountRepository;
import io.github.bytekumar.ledger.transfer.TransferRejectedException.Reason;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;

/**
 * Moves money between two accounts with optimistic locking.
 *
 * Each attempt is its own database transaction. If another transaction changed one of our
 * accounts after we read it, the version check fails, the attempt rolls back (nothing was
 * written), and we retry with fresh data, up to ledger.transfer.max-attempts times.
 */
@Service
public class TransferService {

    private final AccountRepository accounts;
    private final TransferRepository transfers;
    private final TransactionTemplate tx;
    private final int maxAttempts;
    private final Counter versionConflictRetries;
    private final Counter idempotencyRaceRetries;

    public TransferService(AccountRepository accounts, TransferRepository transfers, TransactionTemplate tx,
                           TransferProperties props, MeterRegistry registry) {
        this.accounts = accounts;
        this.transfers = transfers;
        this.tx = tx;
        this.maxAttempts = props.maxAttempts();
        this.versionConflictRetries = Counter.builder("ledger.transfer.retries").tag("reason", "version_conflict")
                .description("Transfer attempts rolled back and retried").register(registry);
        this.idempotencyRaceRetries = Counter.builder("ledger.transfer.retries").tag("reason", "idempotency_race")
                .description("Transfer attempts rolled back and retried").register(registry);
    }

    public record Outcome(Transfer transfer, boolean replayed) {
    }

    public Optional<Transfer> find(long id) {
        return transfers.findById(id);
    }

    public Outcome transfer(TransferCommand cmd) {
        if (cmd.from() == cmd.to()) {
            throw new TransferRejectedException(Reason.SAME_ACCOUNT, "from and to must be different accounts");
        }
        for (int attempt = 1; ; attempt++) {
            try {
                // A RuntimeException thrown inside rolls the whole attempt back.
                return tx.execute(status -> attemptOnce(cmd));
            } catch (RetryableConflict e) {
                if (attempt >= maxAttempts) {
                    throw new ConcurrencyConflictException(attempt);
                }
                (e.versionConflict ? versionConflictRetries : idempotencyRaceRetries).increment();
                backoff(attempt);
            }
        }
    }

    private Outcome attemptOnce(TransferCommand cmd) {
        // 1. Already done under this key? Return the original instead of moving money again.
        Optional<Transfer> existing = transfers.findByKey(cmd.idempotencyKey());
        if (existing.isPresent()) {
            return replay(existing.get(), cmd);
        }

        Account from = accounts.findById(cmd.from()).orElseThrow(() -> new AccountNotFoundException(cmd.from()));
        Account to = accounts.findById(cmd.to()).orElseThrow(() -> new AccountNotFoundException(cmd.to()));

        // 2. Claim the key. Done before the business checks so that a duplicate racing with
        //    the original waits here and then replays, instead of being judged on a balance
        //    the original already changed.
        Transfer transfer = transfers.insertIfKeyUnused(cmd)
                .orElseThrow(() -> new RetryableConflict(false)); // the original just committed; retry to replay it

        // 3. Business rules. Throwing rolls back the key claim too, so a rejected request
        //    can be retried later with the same key.
        if (!from.currency().equals(cmd.currency()) || !to.currency().equals(cmd.currency())) {
            throw new TransferRejectedException(Reason.CURRENCY_MISMATCH, "Transfer currency " + cmd.currency()
                    + " must match both accounts (" + from.currency() + " -> " + to.currency() + ")");
        }
        if (from.balance() < cmd.amount()) {
            throw new TransferRejectedException(Reason.INSUFFICIENT_FUNDS, "Account " + from.id() + " has insufficient funds");
        }

        // 4. Write both balances, lower account id first. Postgres row-locks each updated row
        //    until commit; a fixed order means two transfers over the same pair of accounts
        //    can never each hold one lock while waiting for the other (deadlock).
        Account first = from.id() < to.id() ? from : to;
        Account second = first == from ? to : from;
        long firstDelta = first == from ? -cmd.amount() : cmd.amount();
        updateOrConflict(first, firstDelta);
        updateOrConflict(second, -firstDelta);

        // 5. Double entry: one debit and one credit that sum to zero.
        accounts.insertEntry(from.id(), -cmd.amount(), from.balance() - cmd.amount(), null, transfer.id());
        accounts.insertEntry(to.id(), cmd.amount(), to.balance() + cmd.amount(), null, transfer.id());
        return new Outcome(transfer, false);
    }

    private void updateOrConflict(Account account, long delta) {
        if (!accounts.compareAndSetBalance(account.id(), account.version(), account.balance() + delta)) {
            throw new RetryableConflict(true);
        }
    }

    private Outcome replay(Transfer existing, TransferCommand cmd) {
        if (!existing.matches(cmd)) {
            throw new TransferRejectedException(Reason.IDEMPOTENCY_KEY_REUSED,
                    "Idempotency key was already used for a different transfer (id " + existing.id() + ")");
        }
        return new Outcome(existing, true);
    }

    /** Random sleep up to 2^attempt ms (capped at 64 ms) so colliding threads don't collide again in lockstep. */
    private static void backoff(int attempt) {
        long maxMillis = 1L << Math.min(attempt, 6);
        try {
            Thread.sleep(ThreadLocalRandom.current().nextLong(maxMillis + 1));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted while backing off", e);
        }
    }

    /** Internal signal: this attempt lost a race and was rolled back; try again. */
    private static final class RetryableConflict extends RuntimeException {
        final boolean versionConflict;

        RetryableConflict(boolean versionConflict) {
            super(null, null, false, false); // no stack trace: thrown on a hot path, never logged
            this.versionConflict = versionConflict;
        }
    }
}
