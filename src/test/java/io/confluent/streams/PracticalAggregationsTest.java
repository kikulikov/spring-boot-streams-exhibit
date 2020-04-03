package io.confluent.streams;

import io.confluent.kafka.schemaregistry.testutil.MockSchemaRegistry;
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import io.confluent.model.avro.PracticalEventDetails;
import io.confluent.model.avro.PracticalOnlineEvent;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;

import java.util.*;

import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThat;

@SpringBootTest
class PracticalAggregationsTest {

  private static final String inputTopicName = PracticalAggregations.inputTopicName;
  private static final String outputTopicName = PracticalAggregations.outputTopicName;

  private static final String schemaRegistryScope = PracticalAggregationsTest.class.getName();
  private static final String mockSchemaRegistryURL = "mock://" + schemaRegistryScope;

  private TopologyTestDriver testDriver;
  private TestInputTopic<String, PracticalOnlineEvent> inputTopic;
  private TestOutputTopic<String, PracticalOnlineEvent> outputTopic;

  @Test
  public void shouldHandleSingleMessage() {

    final List<PracticalOnlineEvent> inputValues = new ArrayList<>();
    inputValues.add(new PracticalOnlineEvent("google.com",
        new PracticalEventDetails("test", "customer-1", "user-1", 300L)));

    final Map<String, PracticalOnlineEvent> expectedOutput = new HashMap<>();
    expectedOutput.put("user-1", new PracticalOnlineEvent("google.com",
        new PracticalEventDetails("test", "customer-1", "user-1", 300L)));

    this.inputTopic.pipeValueList(inputValues);
    assertThat(this.outputTopic.readKeyValuesToMap(), equalTo(expectedOutput));
  }

  @Test
  public void shouldAggregateSimilarMessages() {

    final List<PracticalOnlineEvent> inputValues = new ArrayList<>();
    inputValues.add(new PracticalOnlineEvent("io.confluent",
        new PracticalEventDetails("test-1", "customer-1", "user-1", 1L)));
    inputValues.add(new PracticalOnlineEvent("io.confluent",
        new PracticalEventDetails("test-2", "", "user-1", 1L)));

    final List<KeyValue<String, PracticalOnlineEvent>> expectedOutput = new ArrayList<>();
    expectedOutput.add(KeyValue.pair("user-1", new PracticalOnlineEvent("io.confluent",
        new PracticalEventDetails("test-1", "customer-1", "user-1", 1L))));
    expectedOutput.add(KeyValue.pair("user-1", new PracticalOnlineEvent("io.confluent",
        new PracticalEventDetails("test-2", "customer-1", "user-1", 1L))));

    this.inputTopic.pipeValueList(inputValues);
    assertThat(this.outputTopic.readKeyValuesToList(), equalTo(expectedOutput));
  }

  @Test
  public void shouldNotAggregateWhenOrderingWrong() {

    final List<PracticalOnlineEvent> inputValues = new ArrayList<>();
    inputValues.add(new PracticalOnlineEvent("io.confluent",
        new PracticalEventDetails("test-2", "", "user-1", 1L)));
    inputValues.add(new PracticalOnlineEvent("io.confluent",
        new PracticalEventDetails("test-1", "customer-1", "user-1", 1L)));

    final List<KeyValue<String, PracticalOnlineEvent>> expectedOutput = new ArrayList<>();
    expectedOutput.add(KeyValue.pair("user-1", new PracticalOnlineEvent("io.confluent",
        new PracticalEventDetails("test-1", "customer-1", "user-1", 1L))));

    this.inputTopic.pipeValueList(inputValues);
    assertThat(this.outputTopic.readKeyValuesToList(), equalTo(expectedOutput));
  }

  @Test
  public void shouldAggregateMultipleMessages() {

    final List<PracticalOnlineEvent> inputValues = new ArrayList<>();
    inputValues.add(new PracticalOnlineEvent("io.confluent",
        new PracticalEventDetails("test-1-1", "", "user-1", 1L)));
    inputValues.add(new PracticalOnlineEvent("io.confluent",
        new PracticalEventDetails("test-2-1", "", "user-2", 1L)));
    inputValues.add(new PracticalOnlineEvent("io.confluent",
        new PracticalEventDetails("test-1-2", "customer-1", "user-1", 1L)));
    inputValues.add(new PracticalOnlineEvent("io.confluent",
        new PracticalEventDetails("test-2-2", "", "user-2", 1L)));
    inputValues.add(new PracticalOnlineEvent("io.confluent",
        new PracticalEventDetails("test-2-3", "customer-2", "user-2", 1L)));
    inputValues.add(new PracticalOnlineEvent("io.confluent",
        new PracticalEventDetails("test-1-3", "", "user-1", 1L)));
    inputValues.add(new PracticalOnlineEvent("io.confluent",
        new PracticalEventDetails("test-1-4", "", "user-1", 1L)));
    inputValues.add(new PracticalOnlineEvent("io.confluent",
        new PracticalEventDetails("test-2-4", "", "user-2", 1L)));

    final List<KeyValue<String, PracticalOnlineEvent>> expectedOutput = new ArrayList<>();
    expectedOutput.add(KeyValue.pair("user-1", new PracticalOnlineEvent("io.confluent",
        new PracticalEventDetails("test-1-2", "customer-1", "user-1", 1L))));
    expectedOutput.add(KeyValue.pair("user-2", new PracticalOnlineEvent("io.confluent",
        new PracticalEventDetails("test-2-3", "customer-2", "user-2", 1L))));
    expectedOutput.add(KeyValue.pair("user-1", new PracticalOnlineEvent("io.confluent",
        new PracticalEventDetails("test-1-3", "customer-1", "user-1", 1L))));
    expectedOutput.add(KeyValue.pair("user-1", new PracticalOnlineEvent("io.confluent",
        new PracticalEventDetails("test-1-4", "customer-1", "user-1", 1L))));
    expectedOutput.add(KeyValue.pair("user-2", new PracticalOnlineEvent("io.confluent",
        new PracticalEventDetails("test-2-4", "customer-2", "user-2", 1L))));

    this.inputTopic.pipeValueList(inputValues);
    assertThat(this.outputTopic.readKeyValuesToList(), equalTo(expectedOutput));
  }

  @BeforeEach
  void beforeEach() {
    final StreamsBuilder builder = new StreamsBuilder();
    new PracticalAggregations().handleStream(builder); // streams builder injection

    final Topology topology = builder.build();
    final Properties props = topologyConfiguration();

    this.testDriver = new TopologyTestDriver(topology, props);

    final Serde<String> stringSerde = Serdes.String();
    final Serde<PracticalOnlineEvent> onlineEventSerde = new SpecificAvroSerde<>();

    final Map<String, String> schemaConfig = Map.of(
        AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, mockSchemaRegistryURL);
    onlineEventSerde.configure(schemaConfig, false);

    this.inputTopic = testDriver
        .createInputTopic(inputTopicName, stringSerde.serializer(), onlineEventSerde.serializer());

    this.outputTopic = testDriver
        .createOutputTopic(outputTopicName, stringSerde.deserializer(), onlineEventSerde.deserializer());
  }

  @AfterEach
  void afterEach() {
    testDriver.close();
    MockSchemaRegistry.dropScope(schemaRegistryScope);
  }

  private Properties topologyConfiguration() {
    final Properties props = new Properties();
    props.put(StreamsConfig.APPLICATION_ID_CONFIG, "practical-test");
    props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234");
    props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
    props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, SpecificAvroSerde.class);
    props.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, mockSchemaRegistryURL);
    return props;
  }
}
