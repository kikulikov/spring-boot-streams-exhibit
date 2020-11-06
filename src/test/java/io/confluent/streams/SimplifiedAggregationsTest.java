package io.confluent.streams;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Printed;
import org.apache.kafka.streams.kstream.Produced;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;

import java.util.*;

import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThat;

@SpringBootTest
class SimplifiedAggregationsTest {

  private static final String inputTopic = "inputTopic";
  private static final String outputTopic = "outputTopic";

  @Test
  public void shouldAggregate() {

    final List<String> inputValues = Arrays.asList("topology", "kafka", "expression");

    final Map<String, String> expectedOutput = new HashMap<>();
    expectedOutput.put("topology", "topology=8");
    expectedOutput.put("kafka", "kafka=5");
    expectedOutput.put("expression", "expression=10");

    final StreamsBuilder builder = createTopology();
    final Properties streamsConfiguration = createTopologyConfiguration();

    try (final TopologyTestDriver testDriver = new TopologyTestDriver(builder.build(), streamsConfiguration)) {

      final TestInputTopic<String, String> input = testDriver
          .createInputTopic(inputTopic, new StringSerializer(), new StringSerializer());

      final TestOutputTopic<String, String> output = testDriver
          .createOutputTopic(outputTopic, new StringDeserializer(), new StringDeserializer());

      input.pipeValueList(inputValues);

      assertThat(output.readKeyValuesToMap(), equalTo(expectedOutput));
    }
  }

  private StreamsBuilder createTopology() {
    final StreamsBuilder builder = new StreamsBuilder();
    updateTopology(builder);
    return builder;
  }

  private void updateTopology(StreamsBuilder builder) {
    final KStream<String, String> inputStream = builder.stream(inputTopic);
    final KStream<String, String> keyedStream = inputStream.selectKey((a, b) -> b);
    final KTable<String, Integer> pinnedTable = keyedStream.groupBy((k, v) -> v).reduce((z, in) -> in).mapValues(String::length);
    final KStream<String, String> result = keyedStream.join(pinnedTable, (keyed, pinned) -> keyed + "=" + pinned);

    result.print(Printed.toSysOut());
    result.to(outputTopic, Produced.with(Serdes.String(), Serdes.String()));
  }

  private Properties createTopologyConfiguration() {
    final Properties props = new Properties();
    props.put(StreamsConfig.APPLICATION_ID_CONFIG, "simplified-test");
    props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234");
    props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
    props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
    return props;
  }
}