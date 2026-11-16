package io.github.bytekumar.ledger.web;

/** A request that passed bean validation but is still invalid (e.g. unknown currency code). */
public class BadRequestException extends RuntimeException {

    public BadRequestException(String message) {
        super(message);
    }
}
