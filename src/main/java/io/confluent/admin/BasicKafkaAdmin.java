package io.confluent.admin;

import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.KafkaAdmin;

import java.util.HashMap;
import java.util.Map;

@SpringBootApplication
public class BasicKafkaAdmin {

    private static final Logger logger = LoggerFactory.getLogger(BasicKafkaAdmin.class);

    public static void main(String[] args) {
        SpringApplication.run(BasicKafkaAdmin.class, args);
    }

    @Autowired
    private KafkaProperties kafkaProperties;

    @Bean
    @Order(Ordered.HIGHEST_PRECEDENCE)
    @SuppressWarnings("unused")
    public KafkaAdmin kafkaAdmin() {
        final Map<String, Object> configs = new HashMap<>(kafkaProperties.buildAdminProperties());
        configs.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");

        logger.info("Starting the Kafka Admin...");
        return new KafkaAdmin(configs);
    }

    @Bean
    @Order(Ordered.HIGHEST_PRECEDENCE)
    @SuppressWarnings("unused")
    public NewTopic createTopic() {
        logger.info("Creating the topic...");
        return TopicBuilder.name("online-events").partitions(5).replicas(1).build();
    }
}
