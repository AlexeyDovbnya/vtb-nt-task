package ru.dovbnya.kafka_stub;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import tools.jackson.databind.ObjectMapper;

import java.util.concurrent.TimeUnit;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;

import java.time.Duration;

@Component
public class PersonListener {

    private static final Logger log = LoggerFactory.getLogger(PersonListener.class);
    private static final long READ_MIN_MS = 300;   // чтение не быстрее 300 мс
    private static final long WRITE_MIN_MS = 50;   // запись не быстрее 50 мс

    private final ObjectMapper mapper;
    private final PersonRepository repository;

    private final Counter processed;     // обработано сообщений из Kafka
    private final Counter rowsWritten;   // записано строк в БД
    private final Timer readTimer;       // время чтения сообщения
    private final Timer writeTimer;      // время записи строки

    public PersonListener(ObjectMapper mapper, PersonRepository repository, MeterRegistry registry) {
        this.mapper = mapper;
        this.repository = repository;

        this.processed = Counter.builder("stub.kafka.messages.processed")
                .description("Обработано сообщений из Kafka")
                .register(registry);
        this.rowsWritten = Counter.builder("stub.db.rows.written")
                .description("Записано строк в БД")
                .register(registry);
        this.readTimer = Timer.builder("stub.kafka.read")
                .description("Время чтения сообщения из Kafka")
                .publishPercentileHistogram()
                .minimumExpectedValue(Duration.ofMillis(10))
                .maximumExpectedValue(Duration.ofSeconds(2))
                .register(registry);
        this.writeTimer = Timer.builder("stub.db.write")
                .description("Время записи строки в БД")
                .publishPercentileHistogram()
                .minimumExpectedValue(Duration.ofMillis(10))
                .maximumExpectedValue(Duration.ofSeconds(2))
                .register(registry);
    }

    @KafkaListener(topics = "${app.topic}")
    public void onMessage(ConsumerRecord<String, String> record) {
        long readStart = System.nanoTime();
        PersonMessage msg = parse(record.value());
        sleepUntil(readStart, READ_MIN_MS);
        long readNanos = System.nanoTime() - readStart;
        readTimer.record(readNanos, TimeUnit.NANOSECONDS);

        long writeStart = System.nanoTime();
        boolean inserted = repository.insert(msg);
        sleepUntil(writeStart, WRITE_MIN_MS);
        long writeNanos = System.nanoTime() - writeStart;
        writeTimer.record(writeNanos, TimeUnit.NANOSECONDS);

        if (inserted) {
            rowsWritten.increment();
        }
        processed.increment();

        log.info("p={} off={} msg_id={} read={}ms write={}ms inserted={}",
                record.partition(), record.offset(), msg.msgId(),
                TimeUnit.NANOSECONDS.toMillis(readNanos), TimeUnit.NANOSECONDS.toMillis(writeNanos), inserted);
    }

    private PersonMessage parse(String json) {
        try {
            return mapper.readValue(json, PersonMessage.class);
        } catch (Exception e) {
            throw new IllegalArgumentException("Не удалось разобрать сообщение: " + json, e);
        }
    }

    /** Спим остаток, чтобы операция от start заняла не меньше minMs */
    private static void sleepUntil(long startNanos, long minMs) {
        long remaining = minMs - elapsedMs(startNanos);
        if (remaining > 0) {
            try {
                Thread.sleep(remaining);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException("Обработка прервана", e);
            }
        }
    }

    private static long elapsedMs(long startNanos) {
        return TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNanos);
    }
}