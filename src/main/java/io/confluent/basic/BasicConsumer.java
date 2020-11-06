package io.confluent.basic;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class BasicConsumer {

  public static void main(String[] args) throws Exception {
    final Properties props = new Properties();
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "ec2-18-133-73-193.eu-west-2.compute.amazonaws.com:9092");
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);

    final var consumer = new KafkaConsumer<String, String>(props);
    consumer.subscribe(Arrays.asList("bananas"));

    for (int i = 0; i < 10; i++) {
      ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

      for (ConsumerRecord<String, String> record : records) {
        System.out.println("Consuming <<< " + record.offset() + ", " + record.key() + ", " + record.value());
      }

      // consumer.commitSync();
    }
  }
}
