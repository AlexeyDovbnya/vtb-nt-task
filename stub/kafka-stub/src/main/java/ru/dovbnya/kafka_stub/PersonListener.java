package ru.dovbnya.kafka_stub;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class PersonListener {

    private static final Logger log = LoggerFactory.getLogger(PersonListener.class);

    @KafkaListener(topics = "${app.topic}")
    public void onMessage(ConsumerRecord<String, String> record) {
        log.info("partition={} offset={} value={}", record.partition(), record.offset(), record.value());
    }
}