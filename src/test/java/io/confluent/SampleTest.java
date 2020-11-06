package io.confluent;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.ValueTransformer;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.apache.kafka.streams.state.TimestampedKeyValueStore;
import org.junit.jupiter.api.Test;

import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertTrue;

class SampleTest {

  public Topology topology() {

    final StreamsBuilder builder = new StreamsBuilder();

    builder.globalTable("bananas",
        Materialized.<String, String, KeyValueStore<Bytes, byte[]>>as("bananas-store")
            .withKeySerde(Serdes.String()).withValueSerde(Serdes.String()));

    final KStream<String, String> monkeys = builder.stream("monkeys");

    monkeys.transformValues(() -> new ValueTransformer<String, String>() {

      private TimestampedKeyValueStore<String, String> state;

      @Override
      public void init(ProcessorContext processorContext) {
        this.state = (TimestampedKeyValueStore<String, String>) processorContext.getStateStore("bananas-store");
      }

      @Override
      public String transform(String s) {
        // when ReadOnlyKeyValueStore<String, String> -> java.lang.ClassCastException: class org.apache.kafka.streams.state.ValueAndTimestamp cannot be cast to class java.lang.String
        System.out.println(this.state.get(s)); // returns ValueAndTimestamp<String>
        return s;
      }

      @Override
      public void close() {
        // do nothing here
      }
    });

    return builder.build();
  }

  @Test
  void testTopology() {
    Properties props = new Properties();
    props.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, "test");
    props.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234");
    props.setProperty(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
    props.setProperty(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());

    TopologyTestDriver testDriver = new TopologyTestDriver(topology(), props);

    var bananas = testDriver.createInputTopic("bananas",
        Serdes.String().serializer(), Serdes.String().serializer());

    var monkeys = testDriver.createInputTopic("monkeys",
        Serdes.String().serializer(), Serdes.String().serializer());

    bananas.pipeInput("500", "banana-1");
    monkeys.pipeInput("monkey-1", "500");

    assertTrue(true);
  }
}