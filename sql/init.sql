-- Table for valid, processed events
CREATE TABLE IF NOT EXISTS processed_events (
    id          SERIAL PRIMARY KEY,
    event_id    VARCHAR(100) NOT NULL UNIQUE,
    user_id     VARCHAR(100)   NOT NULL,
    event_type  VARCHAR(50)    NOT NULL,
    amount      DECIMAL(10, 2) NOT NULL,
    currency    CHAR(3) NOT NULL,
    event_timestamp_ms BIGINT NOT NULL,
    category    VARCHAR(50),
    ingested_at TIMESTAMP NOT NULL,
    processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    source_topic TEXT NOT NULL,
    source_partition INTEGER NOT NULL,
    source_offset BIGINT NOT NULL
);

-- Index for fast lookups by user
CREATE INDEX IF NOT EXISTS idx_processed_user_id ON processed_events(user_id);
CREATE INDEX IF NOT EXISTS idx_processed_event_type ON processed_events(event_type);
CREATE INDEX IF NOT EXISTS idx_processed_at ON processed_events(processed_at);

-- UTC, event-time windows. Counts are deduplicated within each window.
CREATE TABLE IF NOT EXISTS payment_revenue_windows (
    window_start TIMESTAMP NOT NULL,
    window_end TIMESTAMP NOT NULL,
    currency CHAR(3) NOT NULL,
    payment_count BIGINT NOT NULL,
    revenue NUMERIC(38, 2) NOT NULL,
    PRIMARY KEY (window_start, window_end, currency),
    CHECK (window_end > window_start)
);
CREATE TABLE IF NOT EXISTS activity_windows (
    window_start TIMESTAMP NOT NULL,
    window_end TIMESTAMP NOT NULL,
    event_type VARCHAR(50) NOT NULL,
    currency CHAR(3) NOT NULL,
    event_count BIGINT NOT NULL,
    PRIMARY KEY (window_start, window_end, event_type, currency),
    CHECK (window_end > window_start)
);
