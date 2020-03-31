package io.confluent.data;

import com.fasterxml.jackson.annotation.JsonProperty;

public class PracticalOnlineEvent {

    private final String source;
    private final PracticalEventDetails eventDetails;

    public PracticalOnlineEvent(@JsonProperty("source") final String source,
                                @JsonProperty("eventDetails") final PracticalEventDetails eventDetails) {
        this.source = source;
        this.eventDetails = eventDetails;
    }

    public String getSource() {
        return source;
    }

    public PracticalEventDetails getEventDetails() {
        return eventDetails;
    }

    @Override
    public String toString() {
        return "PracticalOnlineEvent{" +
                "source='" + source + '\'' +
                ", eventDetails=" + eventDetails +
                '}';
    }
}