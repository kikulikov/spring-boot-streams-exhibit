package io.confluent.streams;

import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import io.confluent.model.avro.PracticalEventDetails;
import io.confluent.model.avro.PracticalOnlineEvent;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Printed;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.annotation.EnableKafkaStreams;
import org.springframework.kafka.annotation.KafkaStreamsDefaultConfiguration;
import org.springframework.kafka.config.KafkaStreamsConfiguration;

import java.util.HashMap;
import java.util.Map;

@SpringBootApplication
@EnableKafkaStreams
public class PracticalAggregations {

  private static final Logger logger = LoggerFactory.getLogger(PracticalAggregations.class);
  private static final String schemaRegistryURL = "http://localhost:8081";

  public static final String inputTopicName = "online-events";
  public static final String outputTopicName = "online-events-result";

  @Autowired
  @SuppressWarnings("unused")
  private KafkaProperties kafkaProperties;

  public static void main(String[] args) {
    SpringApplication.run(PracticalAggregations.class, args);
  }

  @Bean
  public KStream<String, PracticalOnlineEvent> handleStream(StreamsBuilder builder) {

    final KStream<String, PracticalOnlineEvent> inputStream = inputStream(builder);
    final KStream<String, PracticalOnlineEvent> keyedStream = keyedStream(inputStream);

    return onlineEventsStream(keyedStream, keyedStream);
  }

  static KStream<String, PracticalOnlineEvent> inputStream(StreamsBuilder builder) {
    return builder.stream(inputTopicName);
  }

  static KStream<String, PracticalOnlineEvent> keyedStream(KStream<String, PracticalOnlineEvent> inputStream) {
    return inputStream.selectKey((a, b) -> b.getEventDetails().getUserId().toString());
  }

  static KStream<String, PracticalOnlineEvent> onlineEventsStream(KStream<String, PracticalOnlineEvent> tableSource,
                                                                  KStream<String, PracticalOnlineEvent> streamSource) {

    final KTable<String, String> userCustomerTable = tableSource
        .filterNot((a, b) -> StringUtils.isBlank(b.getEventDetails().getCustomerId()))
        .groupBy((k, v) -> v.getEventDetails().getUserId().toString())
        .reduce((aggValue, newValue) -> newValue)
        .mapValues(p -> p.getEventDetails().getCustomerId().toString());

    final KStream<String, PracticalOnlineEvent> joinedResult = streamSource.join(userCustomerTable, (practicalOnlineEvent, customerId) -> {
      final PracticalEventDetails eventDetails = practicalOnlineEvent.getEventDetails();
      eventDetails.setCustomerId(customerId);
      practicalOnlineEvent.setEventDetails(eventDetails);

      return practicalOnlineEvent;
    });

    joinedResult.print(Printed.toSysOut());
    joinedResult.to(outputTopicName); // Produced.with(stringSerde, specificAvroSerde)

    return joinedResult;
  }

  @Bean(name = KafkaStreamsDefaultConfiguration.DEFAULT_STREAMS_CONFIG_BEAN_NAME)
  public KafkaStreamsConfiguration config() {
    final Map<String, Object> props = new HashMap<>(kafkaProperties.buildStreamsProperties());
    props.put(StreamsConfig.APPLICATION_ID_CONFIG, "practical-aggregations");
    props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
    props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, SpecificAvroSerde.class);
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
    props.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryURL);

    return new KafkaStreamsConfiguration(props);
  }
}

/* KSQL

->>> STEP 1
-CREATE STREAM onlineevents_str WITH (KAFKA_TOPIC='online-events', VALUE_FORMAT='AVRO');
-
->>> STEP 2
-CREATE STREAM onlineevents_keystr AS SELECT *, EVENTDETAILS->USERID as USERID FROM onlineevents_str partition by USERID;
-
->>> STEP 3
-CREATE TABLE user_cust_mapping WITH (KAFKA_TOPIC='onlineevents_keystr', VALUE_FORMAT='AVRO', KEY='USERID');
-
->>> STEP 4
-CREATE STREAM onlineevents_joinstr AS SELECT *, EVENTDETAILS->USERID as USERID FROM onlineevents_str partition by USERID;
-
->>> STEP 5
-SELECT * FROM ONLINEEVENTS_JOINSTR k INNER JOIN USER_CUST_MAPPING m ON m.USERID = k.USERID;
-
->>> STEP 6
-???

 */