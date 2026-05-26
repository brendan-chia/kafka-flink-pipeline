-- Table for valid, processed events
CREATE TABLE IF NOT EXISTS processed_events (
    id          SERIAL PRIMARY KEY,
    user_id     VARCHAR(100)   NOT NULL,
    event_type  VARCHAR(50)    NOT NULL,
    amount      DECIMAL(10, 2) NOT NULL,
    category    VARCHAR(50),
    processed_at TIMESTAMP      DEFAULT CURRENT_TIMESTAMP
);

-- Index for fast lookups by user
CREATE INDEX IF NOT EXISTS idx_processed_user_id ON processed_events(user_id);
CREATE INDEX IF NOT EXISTS idx_processed_event_type ON processed_events(event_type);