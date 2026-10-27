CREATE TABLE IF NOT EXISTS dialer_stream_sent (
    call_log_id   INTEGER PRIMARY KEY,
    event_id      UUID NOT NULL,
    pubsub_msg_id VARCHAR(100),
    sent_at       TIMESTAMP NOT NULL DEFAULT NOW()
);
