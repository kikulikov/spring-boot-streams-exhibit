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

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

@SpringBootApplication
@EnableKafkaStreams
public class PracticalAggregations {

  private static final Logger logger = LoggerFactory.getLogger(PracticalAggregations.class);
  private static final String inputTopicName = "online-events";
  private static final String outputTopicName = "online-events-result";
  private static final String schemaRegistryURL = "http://localhost:8081";

  @Autowired
  @SuppressWarnings("unused")
  private KafkaProperties kafkaProperties;

  public static void main(String[] args) {
    SpringApplication.run(PracticalAggregations.class, args);
  }

  @Bean
  public KStream<String, PracticalOnlineEvent> handleStream(StreamsBuilder builder) {

    final KStream<String, PracticalOnlineEvent> inputStream = builder.stream(inputTopicName);
    final KStream<String, PracticalOnlineEvent> keyedStream = inputStream
        .selectKey((a, b) -> b.getEventDetails().getUserId().toString());

    final KTable<String, String> userCustomerTable = keyedStream
        .filterNot((a, b) -> StringUtils.isBlank(b.getEventDetails().getCustomerId()))
        .groupBy((k, v) -> v.getEventDetails().getUserId().toString())
        .reduce((aggValue, newValue) -> newValue)
        .mapValues(p -> p.getEventDetails().getCustomerId().toString());

    userCustomerTable.toStream().print(Printed.toSysOut());

    final KStream<String, PracticalOnlineEvent> joinedResult = keyedStream.join(userCustomerTable, (practicalOnlineEvent, customerId) -> {
      final PracticalEventDetails eventDetails = practicalOnlineEvent.getEventDetails();
      eventDetails.setCustomerId(customerId);
      practicalOnlineEvent.setEventDetails(eventDetails);

      System.out.println(">>> " + practicalOnlineEvent.getEventDetails().getUserId() + " " + customerId);

      return practicalOnlineEvent;
    });

    joinedResult.print(Printed.toSysOut());
    joinedResult.to(outputTopicName); // Produced.with(stringSerde, specificAvroSerde)

    return joinedResult;
  }

//  @Bean
//  @SuppressWarnings("unused")
//  public void process() {
//
//    final StreamsBuilder builder = topology();
//    final Properties config = streamsConfig();
//
//    try (final KafkaStreams streams = new KafkaStreams(builder.build(), config)) {
//
//      streams.cleanUp(); // TODO Is it needed actually?
//      streams.start();
//
//    } catch (Exception e) {
//      System.out.println(e.getMessage());
//    }
//  }

//  static StreamsBuilder topology() {
//
//    final StreamsBuilder builder = new StreamsBuilder();
//
//    final Serde<String> stringSerde = Serdes.String();
//    final Serde<PracticalOnlineEvent> specificAvroSerde = new SpecificAvroSerde<>();
//
//    final Map<String, String> schemaConfig = Collections.singletonMap(
//        AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryURL);
//    specificAvroSerde.configure(schemaConfig, false);
//
//    final KStream<String, PracticalOnlineEvent> inputStream = builder.stream(inputTopicName);
//    final KStream<String, PracticalOnlineEvent> keyedStream = inputStream
//        .selectKey((a, b) -> b.getEventDetails().getUserId().toString());
//
//    final KTable<String, String> userCustomerTable = keyedStream
//        .filterNot((a, b) -> StringUtils.isBlank(b.getEventDetails().getCustomerId()))
//        .groupBy((k, v) -> v.getEventDetails().getUserId().toString())
//        .reduce((aggValue, newValue) -> newValue)
//        .mapValues(p -> p.getEventDetails().getCustomerId().toString());
//
//    userCustomerTable.toStream().print(Printed.toSysOut());
//
//    final KStream<String, PracticalOnlineEvent> joinedResult = keyedStream.join(userCustomerTable, (practicalOnlineEvent, customerId) -> {
//      final PracticalEventDetails eventDetails = practicalOnlineEvent.getEventDetails();
//      eventDetails.setCustomerId(customerId);
//      practicalOnlineEvent.setEventDetails(eventDetails);
//
//      System.out.println(">>> " + practicalOnlineEvent.getEventDetails().getUserId() + " " + customerId);
//
//      return practicalOnlineEvent;
//    });
//
//    joinedResult.print(Printed.toSysOut());
//
//    joinedResult.to(inputTopicName + "-result", Produced.with(stringSerde, specificAvroSerde));
//    return builder;
//  }

  private Properties streamsConfig() {
    final Map<String, Object> props = new HashMap<>(kafkaProperties.buildStreamsProperties());
    props.put(StreamsConfig.APPLICATION_ID_CONFIG, "practical-aggregations");
    props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
    props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, SpecificAvroSerde.class);
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    props.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryURL);

    // TODO why is it different for streams???
    final Properties result = new Properties();
    result.putAll(props);

    return result;
  }
}

/*

kafka-avro-console-producer --broker-list localhost:9092 --topic onlineevents --property schema.registry.url=http://localhost:8081 \
--property value.schema='{"type":"record","name":"online-events","namespace":"com.hsbc.avro","fields":[{"name":"source","type":"string"},{"name":"tokenDetails","type":{"type":"record","name":"tokenDetails","fields":[{"name":"tokenModel","type":"string"},{"name":"camLevel","type":"string"},{"name":"idvSessionId","type":"string"},{"name":"sessionCorrelationId","type":"string"}]}},{"name":"eventDetails","type":{"type":"record","name":"eventDetails","fields":[{"name":"eventCode","type":"string"},{"name":"custId","type":"string"},{"name":"userId","type":"string"},{"name":"eventTimestamp","type":"string"}]}}]}'

kafka-avro-console-consumer --bootstrap-server localhost:9092 --topic onlineevents --property schema.registry.url=http://localhost:8081 \
--property value.schema='{"type":"record","name":"onlineevents","namespace":"com.hsbc.avro","fields":[{"name":"source","type":"string"},{"name":"tokenDetails","type":{"type":"record","name":"tokenDetails","fields":[{"name":"tokenModel","type":"string"},{"name":"camLevel","type":"string"},{"name":"idvSessionId","type":"string"},{"name":"sessionCorrelationId","type":"string"}]}},{"name":"eventDetails","type":{"type":"record","name":"eventDetails","fields":[{"name":"eventCode","type":"string"},{"name":"custId","type":"string"},{"name":"userId","type":"string"},{"name":"eventTimestamp","type":"string"}]}}]}' --from-beginning

>>> STEP 1
CREATE STREAM onlineevents_str WITH (KAFKA_TOPIC='onlineevents', VALUE_FORMAT='AVRO');

>>> STEP 2
CREATE STREAM onlineevents_keystr AS SELECT *, EVENTDETAILS->USERID as USERID FROM onlineevents_str partition by USERID;

>>> STEP 3
CREATE TABLE user_cust_mapping WITH (KAFKA_TOPIC='onlineevents_keystr', VALUE_FORMAT='AVRO', KEY='USERID');

>>> STEP 4
CREATE STREAM onlineevents_joinstr AS SELECT *, EVENTDETAILS->USERID as USERID FROM onlineevents_str partition by USERID;

>>> STEP 5
SELECT * FROM ONLINEEVENTS_JOINSTR k INNER JOIN USER_CUST_MAPPING m ON m.USERID = k.USERID;

>>> STEP 6
???

>>> TESTING 1
{"source":"test ","tokenDetails":{"tokenModel":"debv","camLevel":"3","idvSessionId":"","sessionCorrelationId":"34555"},"eventDetails":{"eventCode":"test","custId":"1234567","userId":"ananthESP","eventTimestamp":"2019-09-26T12:00:11.406+00:00"}}

>>> TESTING 2
{"source":"test","tokenDetails":{"tokenModel":"debv","camLevel":"3","idvSessionId":"","sessionCorrelationId":"6789"},"eventDetails":{"eventCode":"test","custId":"","userId":"ananthESP","eventTimestamp":"2019-09-26T12:00:11.406+00:00"}}

*/
