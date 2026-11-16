package io.github.bytekumar.ledger.transfer;

import java.net.URI;

import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import io.github.bytekumar.ledger.Money;
import jakarta.validation.Valid;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Pattern;
import jakarta.validation.constraints.Positive;
import jakarta.validation.constraints.Size;

@RestController
@RequestMapping("/transfers")
public class TransferController {

    private final TransferService service;

    public TransferController(TransferService service) {
        this.service = service;
    }

    public record TransferRequest(
            @NotNull Long from,
            @NotNull Long to,
            @NotNull @Positive @Max(Money.MAX_AMOUNT) Long amount,
            @NotNull @Pattern(regexp = "[A-Z]{3}", message = "must be a 3-letter ISO 4217 code") String currency,
            @NotBlank @Size(max = 128) String idempotencyKey) {
    }

    /** 201 for a new transfer; 200 with Idempotent-Replayed: true when the key was already used for this same transfer. */
    @PostMapping
    ResponseEntity<Transfer> create(@Valid @RequestBody TransferRequest req) {
        TransferService.Outcome out = service.transfer(
                new TransferCommand(req.from(), req.to(), req.amount(), req.currency(), req.idempotencyKey()));
        if (out.replayed()) {
            return ResponseEntity.ok().header("Idempotent-Replayed", "true").body(out.transfer());
        }
        return ResponseEntity.created(URI.create("/transfers/" + out.transfer().id())).body(out.transfer());
    }

    @GetMapping("/{id}")
    ResponseEntity<Transfer> get(@PathVariable long id) {
        return ResponseEntity.of(service.find(id));
    }
}
