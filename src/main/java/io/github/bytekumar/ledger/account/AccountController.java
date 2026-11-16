package io.github.bytekumar.ledger.account;

import java.net.URI;

import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;

import io.github.bytekumar.ledger.account.AccountService.DepositResult;
import io.github.bytekumar.ledger.account.AccountService.EntryPage;
import jakarta.validation.Valid;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Pattern;
import jakarta.validation.constraints.Positive;

@RestController
@RequestMapping("/accounts")
public class AccountController {

    private final AccountService service;

    public AccountController(AccountService service) {
        this.service = service;
    }

    public record CreateAccountRequest(
            @NotBlank String owner,
            @NotNull @Pattern(regexp = "[A-Z]{3}", message = "must be a 3-letter ISO 4217 code") String currency) {
    }

    public record DepositRequest(@NotNull @Positive Long amount) {
    }

    @PostMapping
    ResponseEntity<Account> create(@Valid @RequestBody CreateAccountRequest req) {
        Account account = service.create(req.owner(), req.currency());
        return ResponseEntity.created(URI.create("/accounts/" + account.id())).body(account);
    }

    @GetMapping("/{id}")
    Account get(@PathVariable long id) {
        return service.get(id);
    }

    @PostMapping("/{id}/deposits")
    @ResponseStatus(HttpStatus.CREATED)
    DepositResult deposit(@PathVariable long id, @Valid @RequestBody DepositRequest req) {
        return service.deposit(id, req.amount());
    }

    @GetMapping("/{id}/entries")
    EntryPage entries(@PathVariable long id,
                      @RequestParam(required = false) Long before,
                      @RequestParam(defaultValue = "50") @Min(1) @Max(200) int limit) {
        return service.entries(id, before, limit);
    }
}
