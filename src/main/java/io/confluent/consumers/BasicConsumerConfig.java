package io.confluent.consumers;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;

import java.util.HashMap;
import java.util.Map;

@EnableKafka
@Configuration
public class BasicConsumerConfig {

    private static final Logger logger = LoggerFactory.getLogger(BasicConsumerConfig.class);
    private static final String bootstrapServers = "localhost:9092";
    private static final String groupId = "basic-kafka-consumer";
    private static final String offsetReset = "latest";

    @Autowired
    @SuppressWarnings("unused")
    private KafkaProperties kafkaProperties;

    @Bean
    @SuppressWarnings("unused")
    public ConsumerFactory<Object, Object> consumerFactory() {
        logger.info("Starting the Kafka Consumer Factory...");
        return new DefaultKafkaConsumerFactory<>(consumerConfig());
    }

    public Map<String, Object> consumerConfig() {
        final Map<String, Object> props = new HashMap<>(kafkaProperties.buildConsumerProperties());
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class); // TODO AVRO Serializing
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, offsetReset);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        return props;
    }

    //@Bean
    //@SuppressWarnings("unused")
    //public KafkaListenerContainerFactory<ConcurrentMessageListenerContainer<String, String>> kafkaListenerContainerFactory() {
    //    final ConcurrentKafkaListenerContainerFactory<String, String> factory = new ConcurrentKafkaListenerContainerFactory<>();
    //    factory.setConsumerFactory(consumerFactory());
    //    factory.setConcurrency(4); // TODO Concurrent consumption
    //    return factory;
    //}
}