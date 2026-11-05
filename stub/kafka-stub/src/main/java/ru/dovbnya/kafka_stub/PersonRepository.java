package ru.dovbnya.kafka_stub;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;

@Repository
public class PersonRepository {

    // ON CONFLICT: если Kafka доставит сообщение повторно, дубль не появится
    private static final String INSERT_SQL = """
            INSERT INTO persons (msg_id, full_name, inn, time)
            VALUES (?, ?, ?, ?)
            ON CONFLICT (msg_id) DO NOTHING
            """;

    private final JdbcTemplate jdbc;

    public PersonRepository(JdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    /** @return true — строка вставлена, false — такой msg_id уже есть */
    public boolean insert(PersonMessage msg) {
        LocalDateTime now = LocalDateTime.now().truncatedTo(ChronoUnit.MINUTES); // 2026-02-12 15:06
        int rows = jdbc.update(INSERT_SQL, msg.msgId(), msg.fullName(), msg.inn(), Timestamp.valueOf(now));
        return rows == 1;
    }
}