package io.confluent.producers;

import io.confluent.data.EventSource;
import io.confluent.model.avro.PracticalOnlineEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;

@ComponentScan({"io.confluent.data", "io.confluent.producers"})
@SpringBootApplication
@EnableScheduling
public class BasicProducerApp {

  private static final Logger logger = LoggerFactory.getLogger(BasicProducerApp.class);
  private static final String topicName = "online-events";

  public static void main(String[] args) {
    SpringApplication.run(BasicProducerApp.class, args);
  }

  @Autowired
  @SuppressWarnings("unused")
  private KafkaTemplate<String, Object> kafkaTemplate;

  @Autowired
  @SuppressWarnings("unused")
  private EventSource eventSource;

  @Scheduled(initialDelay = 500, fixedRate = 2000)
  @SuppressWarnings("unused")
  public void produce() {
    final PracticalOnlineEvent event = eventSource.retrieveEvent();

    logger.info("Sending='{}'", event);
    kafkaTemplate.send(topicName, event);
  }
}
