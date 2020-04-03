package io.confluent.data;

import com.github.javafaker.Faker;
import io.confluent.model.avro.PracticalEventDetails;
import io.confluent.model.avro.PracticalOnlineEvent;
import org.springframework.stereotype.Component;

import java.util.concurrent.TimeUnit;

@Component
public class EventSourceFaker implements EventSource {

  private final Faker faker = new Faker();

  public PracticalOnlineEvent retrieveEvent() {
    return new PracticalOnlineEvent(faker.internet().url(), newEventDetails());
  }

  private PracticalEventDetails newEventDetails() {
    return new PracticalEventDetails(faker.internet().macAddress(),
        perhapsCustomerId(), faker.harryPotter().character(), recentTimestamp());
  }

  private String perhapsCustomerId() {
    return (faker.random().nextInt(5) % 3 == 0) ? faker.internet().uuid() : "";
  }

  private long recentTimestamp() {
    return faker.date().past(5, TimeUnit.MINUTES).getTime();
  }
}
