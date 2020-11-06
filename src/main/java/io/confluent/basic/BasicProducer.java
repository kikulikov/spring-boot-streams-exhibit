package io.confluent.basic;

import com.github.javafaker.Faker;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.concurrent.Future;

public class BasicProducer {

  private static final Faker faker = new Faker();

  public static void main(String[] args) throws Exception {
    final Properties props = new Properties();
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "ec2-18-133-73-193.eu-west-2.compute.amazonaws.com:9092");
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
    props.put(ProducerConfig.ACKS_CONFIG, "all");

    final var producer = new KafkaProducer<String, String>(props);

    final var character = faker.harryPotter().character();
    final var spell = faker.harryPotter().spell();
    final var record = new ProducerRecord<>("bananas", character, spell);

    final Future<RecordMetadata> future = producer.send(record);
    final RecordMetadata metadata = future.get();

    System.out.println("Producing >>> " + metadata);
  }
}
