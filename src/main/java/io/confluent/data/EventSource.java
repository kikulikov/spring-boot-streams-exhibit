package io.confluent.data;

import org.springframework.stereotype.Service;

@Service
public interface EventSource {

  PracticalOnlineEvent retrieveEvent();
}
