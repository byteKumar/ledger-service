package io.github.bytekumar.ledger.account;

public class AccountNotFoundException extends RuntimeException {

    public AccountNotFoundException(long id) {
        super("Account " + id + " not found");
    }
}
