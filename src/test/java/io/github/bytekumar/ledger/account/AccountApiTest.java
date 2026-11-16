package io.github.bytekumar.ledger.account;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;

import io.github.bytekumar.ledger.IntegrationTest;

@SuppressWarnings({"rawtypes", "unchecked"})
class AccountApiTest extends IntegrationTest {

    private Map<?, ?> createAccount(String owner, String currency) {
        ResponseEntity<Map> res = http.postForEntity("/accounts", Map.of("owner", owner, "currency", currency), Map.class);
        assertThat(res.getStatusCode()).isEqualTo(HttpStatus.CREATED);
        return res.getBody();
    }

    private long id(Map<?, ?> account) {
        return ((Number) account.get("id")).longValue();
    }

    @Test
    void createsAccountWithZeroBalance() {
        ResponseEntity<Map> res = http.postForEntity("/accounts", Map.of("owner", "alice", "currency", "USD"), Map.class);

        assertThat(res.getStatusCode()).isEqualTo(HttpStatus.CREATED);
        assertThat(res.getHeaders().getLocation()).hasToString("/accounts/" + id(res.getBody()));
        assertThat(res.getBody()).containsEntry("owner", "alice").containsEntry("currency", "USD").containsEntry("balance", 0);
    }

    @Test
    void getsAccountById() {
        long id = id(createAccount("bob", "EUR"));

        ResponseEntity<Map> res = http.getForEntity("/accounts/" + id, Map.class);

        assertThat(res.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(res.getBody()).containsEntry("currency", "EUR").containsEntry("balance", 0);
    }

    @Test
    void unknownAccountIs404() {
        ResponseEntity<Map> res = http.getForEntity("/accounts/999999", Map.class);

        assertThat(res.getStatusCode()).isEqualTo(HttpStatus.NOT_FOUND);
        assertThat(res.getBody()).containsEntry("detail", "Account 999999 not found");
    }

    @Test
    void rejectsMalformedAndUnknownCurrencies() {
        assertThat(http.postForEntity("/accounts", Map.of("owner", "x", "currency", "usd"), Map.class).getStatusCode())
                .isEqualTo(HttpStatus.BAD_REQUEST);
        assertThat(http.postForEntity("/accounts", Map.of("owner", "x", "currency", "ABC"), Map.class).getStatusCode())
                .isEqualTo(HttpStatus.BAD_REQUEST);
        assertThat(http.postForEntity("/accounts", Map.of("currency", "USD"), Map.class).getStatusCode())
                .isEqualTo(HttpStatus.BAD_REQUEST);
    }

    @Test
    void depositIncreasesBalanceAndWritesEntry() {
        long id = id(createAccount("carol", "USD"));

        ResponseEntity<Map> dep = http.postForEntity("/accounts/" + id + "/deposits", Map.of("amount", 1_500), Map.class);

        assertThat(dep.getStatusCode()).isEqualTo(HttpStatus.CREATED);
        assertThat(dep.getBody()).containsEntry("balanceAfter", 1_500);
        assertThat(http.getForObject("/accounts/" + id, Map.class)).containsEntry("balance", 1_500);
        List<?> items = (List<?>) http.getForObject("/accounts/" + id + "/entries", Map.class).get("items");
        assertThat(items).hasSize(1);
        assertThat((Map<String, Object>) items.get(0)).containsEntry("amount", 1_500).containsEntry("balanceAfter", 1_500);
    }

    @Test
    void rejectsNonPositiveDeposit() {
        long id = id(createAccount("dan", "USD"));

        assertThat(http.postForEntity("/accounts/" + id + "/deposits", Map.of("amount", 0), Map.class).getStatusCode())
                .isEqualTo(HttpStatus.BAD_REQUEST);
        assertThat(http.postForEntity("/accounts/" + id + "/deposits", Map.of("amount", -5), Map.class).getStatusCode())
                .isEqualTo(HttpStatus.BAD_REQUEST);
        assertThat(http.getForObject("/accounts/" + id, Map.class)).containsEntry("balance", 0);
    }

    @Test
    void depositToUnknownAccountIs404() {
        assertThat(http.postForEntity("/accounts/999999/deposits", Map.of("amount", 10), Map.class).getStatusCode())
                .isEqualTo(HttpStatus.NOT_FOUND);
    }

    @Test
    void entriesArePaginatedNewestFirst() {
        long id = id(createAccount("erin", "USD"));
        for (int amount = 1; amount <= 5; amount++) {
            http.postForEntity("/accounts/" + id + "/deposits", Map.of("amount", amount), Map.class);
        }

        Map<?, ?> page1 = http.getForObject("/accounts/" + id + "/entries?limit=2", Map.class);
        Map<?, ?> page2 = http.getForObject("/accounts/" + id + "/entries?limit=2&before=" + page1.get("nextBefore"), Map.class);
        Map<?, ?> page3 = http.getForObject("/accounts/" + id + "/entries?limit=2&before=" + page2.get("nextBefore"), Map.class);

        assertThat(amounts(page1)).containsExactly(5, 4);
        assertThat(amounts(page2)).containsExactly(3, 2);
        assertThat(amounts(page3)).containsExactly(1);
        assertThat(page3.get("nextBefore")).isNull();
    }

    @Test
    void entriesLimitIsBounded() {
        long id = id(createAccount("fay", "USD"));

        assertThat(http.getForEntity("/accounts/" + id + "/entries?limit=0", Map.class).getStatusCode())
                .isEqualTo(HttpStatus.BAD_REQUEST);
        assertThat(http.getForEntity("/accounts/" + id + "/entries?limit=201", Map.class).getStatusCode())
                .isEqualTo(HttpStatus.BAD_REQUEST);
    }

    private List<Object> amounts(Map<?, ?> page) {
        return ((List<?>) page.get("items")).stream().map(e -> (Object) ((Map<?, ?>) e).get("amount")).toList();
    }
}
