package io.confluent.consumers;

import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.Test;
import org.springframework.kafka.listener.ConsumerAwareRebalanceListener;

import java.time.Duration;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

//@RunWith(SpringRunner.class)
//@SpringBootTest
public class TimeConsumer {

  static class MyRebalanceListener implements ConsumerAwareRebalanceListener {

    @Override
    public void onPartitionsAssigned(Consumer<?, ?> consumer, Collection<TopicPartition> partitions) {
      long rewindTo = System.currentTimeMillis() - 600000;
      Map<TopicPartition, OffsetAndTimestamp> offsetsForTimes = consumer.offsetsForTimes(partitions.stream()
          .collect(Collectors.toMap(tp -> tp, tp -> rewindTo)));

      offsetsForTimes.forEach((k, v) -> System.out.println(">>> " + k + "::" + v));
      offsetsForTimes.forEach((k, v) -> {
        if (v != null) consumer.seek(k, v.offset());
      });
    }
  }

  @Test
  public void testTest() {

    final Properties props = new Properties();
    props.putAll(consumerConfig());

    final KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
    consumer.subscribe(Collections.singletonList("online-events"));
    consumer.poll(Duration.ofMillis(0));

    final Stream<TopicPartition> partitionsStream = consumer.listTopics().get("online-events")
        .stream().map(s -> new TopicPartition(s.topic(), s.partition()));

    final MyRebalanceListener listener = new MyRebalanceListener();
    listener.onPartitionsAssigned(consumer, partitionsStream.collect(Collectors.toList()));

    while (true) {
      ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(500));
      records.forEach(c -> System.out.println(">>> " + c));
    }
  }

  private static final String bootstrapServers = "localhost:9092";
  private static final String schemaRegistryURL = "http://localhost:8081";
  private static final String groupId = "test-consumer";
  private static final String offsetReset = "latest";

  public Map<String, Object> consumerConfig() {
    final Map<String, Object> props = new HashMap<>();
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class);
    props.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryURL);
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, offsetReset);
    props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
    return props;
  }
}
