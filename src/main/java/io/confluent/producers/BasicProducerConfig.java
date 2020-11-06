package io.confluent.producers;

import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;

import java.util.HashMap;
import java.util.Map;

@Configuration
public class BasicProducerConfig {

  private static final Logger logger = LoggerFactory.getLogger(BasicProducerConfig.class);
  private static final String bootstrapServers = "ec2-18-133-140-153.eu-west-2.compute.amazonaws.com:9092";
  private static final String schemaRegistryURL = "http://ec2-18-133-140-8.eu-west-2.compute.amazonaws.com:8081";

  @Autowired
  @SuppressWarnings("unused")
  private KafkaProperties kafkaProperties;

  @Bean
  @SuppressWarnings("unused")
  public KafkaTemplate<?, ?> kafkaTemplate() {
    return new KafkaTemplate<>(producerFactory());
  }

  @Bean
  public ProducerFactory<?, ?> producerFactory() {
    logger.info("Starting the Kafka Producer Factory...");
    return new DefaultKafkaProducerFactory<>(producerConfig());
  }

  private Map<String, Object> producerConfig() {
    final Map<String, Object> props = new HashMap<>(kafkaProperties.buildProducerProperties());
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    props.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryURL);
    return props;
  }
}
