package io.confluent;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.ValueTransformer;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;

import java.util.Properties;

public class Sample {

  // docker exec -t spring-boot-streams-exhibit_kafka-0_1 kafka-topics --bootstrap-server :19092 --create --topic monkeys --replication-factor 2 --partitions 2
  // docker exec -t spring-boot-streams-exhibit_kafka-0_1 kafka-topics --bootstrap-server :19092 --create --topic monkeys --replication-factor 2 --partitions 2
  // docker exec -t spring-boot-streams-exhibit_kafka-0_1 kafka-topics --bootstrap-server :19092 --list

  // docker exec -it spring-boot-streams-exhibit_kafka-0_1 kafka-console-producer --broker-list :19092 --topic bananas --property "parse.key=true" --property "key.separator=:"
  // docker exec -it spring-boot-streams-exhibit_kafka-0_1 kafka-console-consumer --bootstrap-server :19092 --topic bananas --from-beginning --property print.key=true --property key.separator=":"

  // docker exec -it spring-boot-streams-exhibit_kafka-0_1 kafka-console-producer --broker-list :19092 --topic bananas --property "parse.key=true" --property "key.separator=:"
  // docker exec -it spring-boot-streams-exhibit_kafka-0_1 kafka-console-consumer --bootstrap-server :19092 --topic bananas --from-beginning --property print.key=true --property key.separator=":"

  public static void main(String[] args) throws Exception {

    final Properties props = new Properties();
    props.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, "bananas-service");
    props.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    props.setProperty(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
    props.setProperty(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());

//    final Topology topology = topology();
//    final KafkaStreams streams = new KafkaStreams(topology, props);
//    streams.start();
  }
}
