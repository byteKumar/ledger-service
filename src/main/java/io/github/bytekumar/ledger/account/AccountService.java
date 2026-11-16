package io.github.bytekumar.ledger.account;

import java.util.Currency;
import java.util.List;

import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import io.github.bytekumar.ledger.web.BadRequestException;

@Service
public class AccountService {

    private final AccountRepository accounts;

    public AccountService(AccountRepository accounts) {
        this.accounts = accounts;
    }

    public Account create(String owner, String currency) {
        try {
            Currency.getInstance(currency); // rejects codes that aren't ISO 4217
        } catch (IllegalArgumentException e) {
            throw new BadRequestException("Unknown currency: " + currency);
        }
        return accounts.insert(owner, currency);
    }

    public Account get(long id) {
        return accounts.findById(id).orElseThrow(() -> new AccountNotFoundException(id));
    }

    /** Deposit row, balance update and entry commit together or not at all. */
    @Transactional
    public DepositResult deposit(long accountId, long amount) {
        get(accountId);
        long balanceAfter = accounts.addToBalance(accountId, amount);
        long depositId = accounts.insertDeposit(accountId, amount);
        accounts.insertEntry(accountId, amount, balanceAfter, depositId, null);
        return new DepositResult(depositId, accountId, amount, balanceAfter);
    }

    /** Fetches one extra row to know whether another page exists, without a COUNT query. */
    public EntryPage entries(long accountId, Long beforeId, int limit) {
        get(accountId);
        List<Entry> rows = accounts.findEntries(accountId, beforeId, limit + 1);
        boolean hasMore = rows.size() > limit;
        List<Entry> page = hasMore ? rows.subList(0, limit) : rows;
        Long nextBefore = hasMore ? page.get(page.size() - 1).id() : null;
        return new EntryPage(page, nextBefore);
    }

    public record DepositResult(long depositId, long accountId, long amount, long balanceAfter) {
    }

    public record EntryPage(List<Entry> items, Long nextBefore) {
    }
}
