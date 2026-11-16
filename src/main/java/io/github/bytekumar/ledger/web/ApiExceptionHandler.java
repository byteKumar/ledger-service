package io.github.bytekumar.ledger.web;

import org.springframework.http.HttpStatus;
import org.springframework.http.ProblemDetail;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

import io.github.bytekumar.ledger.account.AccountNotFoundException;
import io.github.bytekumar.ledger.transfer.ConcurrencyConflictException;
import io.github.bytekumar.ledger.transfer.TransferRejectedException;

/**
 * Maps domain exceptions to RFC 9457 problem+json responses. Bean-validation failures
 * (@Valid) are already turned into 400 problem responses by Spring
 * (spring.mvc.problemdetails.enabled=true).
 */
@RestControllerAdvice
public class ApiExceptionHandler {

    @ExceptionHandler(AccountNotFoundException.class)
    ProblemDetail notFound(AccountNotFoundException e) {
        return ProblemDetail.forStatusAndDetail(HttpStatus.NOT_FOUND, e.getMessage());
    }

    @ExceptionHandler(BadRequestException.class)
    ProblemDetail badRequest(BadRequestException e) {
        return ProblemDetail.forStatusAndDetail(HttpStatus.BAD_REQUEST, e.getMessage());
    }

    /** Adds a machine-readable "code" (e.g. INSUFFICIENT_FUNDS) next to the human message. */
    @ExceptionHandler(TransferRejectedException.class)
    ProblemDetail transferRejected(TransferRejectedException e) {
        ProblemDetail pd = ProblemDetail.forStatusAndDetail(e.reason().status, e.getMessage());
        pd.setProperty("code", e.reason().name());
        return pd;
    }

    /** 503 = temporary; the client should retry with the same idempotency key. */
    @ExceptionHandler(ConcurrencyConflictException.class)
    ResponseEntity<ProblemDetail> concurrencyConflict(ConcurrencyConflictException e) {
        ProblemDetail pd = ProblemDetail.forStatusAndDetail(HttpStatus.SERVICE_UNAVAILABLE, e.getMessage());
        pd.setProperty("code", "CONCURRENCY_CONFLICT");
        return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE).header("Retry-After", "1").body(pd);
    }
}
