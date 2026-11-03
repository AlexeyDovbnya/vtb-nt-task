CREATE TABLE IF NOT EXISTS persons (
    id        BIGSERIAL    PRIMARY KEY,
    msg_id    VARCHAR(36)  NOT NULL UNIQUE,
    full_name VARCHAR(255) NOT NULL,
    inn       VARCHAR(12)  NOT NULL,
    time      TIMESTAMP(0) NOT NULL
);