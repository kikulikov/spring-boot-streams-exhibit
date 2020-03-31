package io.confluent.consumers;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.annotation.KafkaListener;

@SpringBootApplication
public class BasicConsumerApp {

  private static final Logger logger = LoggerFactory.getLogger(BasicConsumerApp.class);
  private static final String topicName = "online-events";

  public static void main(String[] args) {
    SpringApplication.run(BasicConsumerApp.class, args);
  }

  @KafkaListener(topics = topicName)
  @SuppressWarnings("unused")
  public void receive(String payload) {
    logger.info("Received='{}'", payload);
  }
}