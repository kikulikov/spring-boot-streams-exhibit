package io.confluent.data;

import com.fasterxml.jackson.annotation.JsonProperty;

public class PracticalEventDetails {

    private final String eventCode;
    private final String customerId;
    private final String userId;

    public PracticalEventDetails(@JsonProperty("eventCode") final String eventCode,
                                 @JsonProperty("custId") final String customerId,
                                 @JsonProperty("userId") final String userId) {
        this.eventCode = eventCode;
        this.customerId = customerId;
        this.userId = userId;
    }

    public String getEventCode() {
        return eventCode;
    }

    public String getCustomerId() {
        return customerId;
    }

    public String getUserId() {
        return userId;
    }

    @Override
    public String toString() {
        return "PracticalEventDetails{" +
                "eventCode='" + eventCode + '\'' +
                ", customerId='" + customerId + '\'' +
                ", userId='" + userId + '\'' +
                '}';
    }
}