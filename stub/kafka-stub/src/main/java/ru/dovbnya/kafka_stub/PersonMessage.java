package ru.dovbnya.kafka_stub;

import com.fasterxml.jackson.annotation.JsonProperty;

// { "msg_id": "...", "full_name": "...", "inn": "..." }
public record PersonMessage(
        @JsonProperty("msg_id") String msgId,
        @JsonProperty("full_name") String fullName,
        @JsonProperty("inn") String inn) {
}