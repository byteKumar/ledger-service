package io.github.bytekumar.ledger.transfer;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;

import io.github.bytekumar.ledger.IntegrationTest;

@SuppressWarnings({"rawtypes", "unchecked"})
class TransferApiTest extends IntegrationTest {

    private long account(String currency, long deposit) {
        Map acc = http.postForObject("/accounts", Map.of("owner", "t", "currency", currency), Map.class);
        long id = ((Number) acc.get("id")).longValue();
        if (deposit > 0) {
            http.postForEntity("/accounts/" + id + "/deposits", Map.of("amount", deposit), Map.class);
        }
        return id;
    }

    private long balance(long id) {
        return ((Number) http.getForObject("/accounts/" + id, Map.class).get("balance")).longValue();
    }

    private ResponseEntity<Map> transfer(long from, long to, long amount, String currency, String key) {
        Map<String, Object> body = new HashMap<>();
        body.put("from", from);
        body.put("to", to);
        body.put("amount", amount);
        body.put("currency", currency);
        body.put("idempotencyKey", key);
        return http.postForEntity("/transfers", body, Map.class);
    }

    private long transferCount() {
        return jdbc.sql("SELECT count(*) FROM transfers").query(Long.class).single();
    }

    @Test
    void movesMoneyAndWritesBalancedEntries() {
        long a = account("USD", 1_000);
        long b = account("USD", 0);

        ResponseEntity<Map> res = transfer(a, b, 300, "USD", "k1");

        assertThat(res.getStatusCode()).isEqualTo(HttpStatus.CREATED);
        assertThat(balance(a)).isEqualTo(700);
        assertThat(balance(b)).isEqualTo(300);
        long transferId = ((Number) res.getBody().get("id")).longValue();
        List<Long> amounts = jdbc.sql("SELECT amount FROM entries WHERE transfer_id = :id ORDER BY amount")
                .param("id", transferId).query(Long.class).list();
        assertThat(amounts).containsExactly(-300L, 300L);
        assertThat(http.getForEntity("/transfers/" + transferId, Map.class).getStatusCode()).isEqualTo(HttpStatus.OK);
    }

    @Test
    void sameKeyReplaysOriginalAndMovesMoneyOnce() {
        long a = account("USD", 1_000);
        long b = account("USD", 0);

        ResponseEntity<Map> first = transfer(a, b, 250, "USD", "same-key");
        ResponseEntity<Map> second = transfer(a, b, 250, "USD", "same-key");

        assertThat(first.getStatusCode()).isEqualTo(HttpStatus.CREATED);
        assertThat(second.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(second.getHeaders().getFirst("Idempotent-Replayed")).isEqualTo("true");
        assertThat(second.getBody().get("id")).isEqualTo(first.getBody().get("id"));
        assertThat(balance(a)).isEqualTo(750);
        assertThat(balance(b)).isEqualTo(250);
        assertThat(transferCount()).isEqualTo(1);
    }

    @Test
    void sameKeyWithDifferentBodyIs409() {
        long a = account("USD", 1_000);
        long b = account("USD", 0);
        transfer(a, b, 100, "USD", "k");

        ResponseEntity<Map> res = transfer(a, b, 999, "USD", "k");

        assertThat(res.getStatusCode()).isEqualTo(HttpStatus.CONFLICT);
        assertThat(res.getBody()).containsEntry("code", "IDEMPOTENCY_KEY_REUSED");
        assertThat(balance(a)).isEqualTo(900);
    }

    @Test
    void insufficientFundsIs422AndChangesNothing() {
        long a = account("USD", 100);
        long b = account("USD", 0);

        ResponseEntity<Map> res = transfer(a, b, 101, "USD", "k");

        assertThat(res.getStatusCode()).isEqualTo(HttpStatus.UNPROCESSABLE_ENTITY);
        assertThat(res.getBody()).containsEntry("code", "INSUFFICIENT_FUNDS");
        assertThat(balance(a)).isEqualTo(100);
        assertThat(balance(b)).isZero();
        assertThat(transferCount()).isZero();
    }

    @Test
    void rejectedTransferDoesNotBurnTheKey() {
        long a = account("USD", 100);
        long b = account("USD", 0);
        assertThat(transfer(a, b, 150, "USD", "k").getStatusCode()).isEqualTo(HttpStatus.UNPROCESSABLE_ENTITY);

        http.postForEntity("/accounts/" + a + "/deposits", Map.of("amount", 100), Map.class);

        assertThat(transfer(a, b, 150, "USD", "k").getStatusCode()).isEqualTo(HttpStatus.CREATED);
        assertThat(balance(b)).isEqualTo(150);
    }

    @Test
    void currencyMismatchesAreRejected() {
        long usd = account("USD", 1_000);
        long eur = account("EUR", 1_000);
        long usd2 = account("USD", 0);

        ResponseEntity<Map> crossCurrency = transfer(usd, eur, 10, "USD", "k1");
        ResponseEntity<Map> wrongRequestCurrency = transfer(usd, usd2, 10, "EUR", "k2");

        assertThat(crossCurrency.getStatusCode()).isEqualTo(HttpStatus.UNPROCESSABLE_ENTITY);
        assertThat(crossCurrency.getBody()).containsEntry("code", "CURRENCY_MISMATCH");
        assertThat(wrongRequestCurrency.getStatusCode()).isEqualTo(HttpStatus.UNPROCESSABLE_ENTITY);
        assertThat(balance(usd)).isEqualTo(1_000);
        assertThat(transferCount()).isZero();
    }

    @Test
    void sameAccountIs400() {
        long a = account("USD", 100);

        ResponseEntity<Map> res = transfer(a, a, 10, "USD", "k");

        assertThat(res.getStatusCode()).isEqualTo(HttpStatus.BAD_REQUEST);
        assertThat(res.getBody()).containsEntry("code", "SAME_ACCOUNT");
    }

    @Test
    void unknownAccountIs404() {
        long a = account("USD", 100);

        assertThat(transfer(a, 999_999, 10, "USD", "k").getStatusCode()).isEqualTo(HttpStatus.NOT_FOUND);
        assertThat(transfer(999_999, a, 10, "USD", "k").getStatusCode()).isEqualTo(HttpStatus.NOT_FOUND);
        assertThat(transferCount()).isZero();
    }

    @Test
    void invalidRequestsAre400() {
        long a = account("USD", 100);
        long b = account("USD", 0);

        assertThat(transfer(a, b, 0, "USD", "k").getStatusCode()).isEqualTo(HttpStatus.BAD_REQUEST);
        assertThat(transfer(a, b, -1, "USD", "k").getStatusCode()).isEqualTo(HttpStatus.BAD_REQUEST);
        assertThat(transfer(a, b, 10, "USD", " ").getStatusCode()).isEqualTo(HttpStatus.BAD_REQUEST);
        assertThat(transfer(a, b, 10, "usd", "k").getStatusCode()).isEqualTo(HttpStatus.BAD_REQUEST);
    }

    @Test
    void oversizedAmountsAre400NotA500() {
        // Milestone 1 exercise: Long.MAX_VALUE then 1 used to overflow the BIGINT balance (500).
        long a = account("USD", 0);

        ResponseEntity<Map> deposit = http.postForEntity("/accounts/" + a + "/deposits", Map.of("amount", Long.MAX_VALUE), Map.class);

        assertThat(deposit.getStatusCode()).isEqualTo(HttpStatus.BAD_REQUEST);
        assertThat(balance(a)).isZero();
        assertThat(transfer(a, account("USD", 0), Long.MAX_VALUE, "USD", "k").getStatusCode()).isEqualTo(HttpStatus.BAD_REQUEST);
    }
}
