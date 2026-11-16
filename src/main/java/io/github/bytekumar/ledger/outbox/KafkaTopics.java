package io.github.bytekumar.ledger.outbox;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

@Configuration
public class KafkaTopics {

    public static final String TRANSFERS = "ledger.transfers";

    /** Created on startup if missing. Keyed by transfer id, so 3 partitions spread the load. */
    @Bean
    NewTopic transfersTopic() {
        return TopicBuilder.name(TRANSFERS).partitions(3).replicas(1).build();
    }
}
