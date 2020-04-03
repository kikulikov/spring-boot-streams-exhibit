package io.confluent.data;

import io.confluent.model.avro.PracticalOnlineEvent;
import org.springframework.stereotype.Service;

@Service
public interface EventSource {

  PracticalOnlineEvent retrieveEvent();
}
