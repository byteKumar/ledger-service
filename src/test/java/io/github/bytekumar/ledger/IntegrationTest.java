package io.github.bytekumar.ledger;

import org.junit.jupiter.api.BeforeEach;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.kafka.KafkaContainer;

/**
 * Base class for tests that need the real app, a real Postgres and a real Kafka. One of each is
 * started for the whole test run (static field) and Spring caches the app context
 * across test classes, so only the first test pays the startup cost.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
public abstract class IntegrationTest {

    @ServiceConnection
    static final PostgreSQLContainer<?> POSTGRES = new PostgreSQLContainer<>("postgres:14-alpine");

    /** Same Kafka version as the Homebrew broker used for the benchmarks (KRaft, no ZooKeeper). */
    @ServiceConnection
    static final KafkaContainer KAFKA = new KafkaContainer("apache/kafka:4.4.0");

    static {
        POSTGRES.start();
        KAFKA.start();
    }

    @Autowired
    protected TestRestTemplate http;

    @Autowired
    protected JdbcClient jdbc;

    @BeforeEach
    void cleanDatabase() {
        jdbc.sql("TRUNCATE accounts, deposits, transfers, entries, outbox_events, consumer_processed_events RESTART IDENTITY CASCADE").update();
    }
}
