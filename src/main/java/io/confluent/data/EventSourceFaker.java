package io.confluent.data;

import com.github.javafaker.Faker;
import org.springframework.stereotype.Component;

@Component
public class EventSourceFaker implements EventSource {

  private final Faker faker = new Faker();

  public PracticalOnlineEvent retrieveEvent() {
    return new PracticalOnlineEvent(faker.internet().url(), newEventDetails());
  }

  private PracticalEventDetails newEventDetails() {
    return new PracticalEventDetails(faker.internet().macAddress(),
        faker.internet().uuid(), faker.harryPotter().character());
  }
}
