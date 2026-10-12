-- Durable receipts are deliberately NOT pruned with the bounded event-ID index.
CREATE TABLE IF NOT EXISTS journeypulse.customers (
    user_id TEXT PRIMARY KEY,
    preferred_language TEXT,
    marketing_consent BOOLEAN NOT NULL DEFAULT FALSE,
    consent_updated_at TIMESTAMPTZ,
    onboarded_at TIMESTAMPTZ,
    attributes_updated_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);
CREATE TABLE IF NOT EXISTS journeypulse.behavioral_summaries (
    user_id TEXT PRIMARY KEY REFERENCES journeypulse.customers(user_id),
    event_counts JSONB NOT NULL DEFAULT '{}',
    completed_orders BIGINT NOT NULL DEFAULT 0,
    latest_activity_at TIMESTAMPTZ,
    latest_order_at TIMESTAMPTZ,
    latest_received_at TIMESTAMPTZ,
    processed_at TIMESTAMPTZ,
    revision BIGINT NOT NULL DEFAULT 0
);
CREATE TABLE IF NOT EXISTS journeypulse.kafka_receipts (
    topic TEXT NOT NULL,
    partition_id INTEGER NOT NULL CHECK (partition_id >= 0),
    offset_id BIGINT NOT NULL CHECK (offset_id >= 0),
    payload_hash TEXT NOT NULL,
    outcome TEXT NOT NULL CHECK (outcome IN ('applied', 'duplicate', 'conflict', 'invalid')),
    processed_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY(topic, partition_id, offset_id)
);
CREATE TABLE IF NOT EXISTS journeypulse.event_ids (
    event_id TEXT PRIMARY KEY,
    payload_hash TEXT NOT NULL,
    expires_at TIMESTAMPTZ NOT NULL
);
CREATE INDEX IF NOT EXISTS event_ids_expiry ON journeypulse.event_ids(expires_at);
CREATE TABLE IF NOT EXISTS journeypulse.completed_orders (
    user_id TEXT NOT NULL REFERENCES journeypulse.customers(user_id),
    order_id TEXT NOT NULL,
    occurred_at TIMESTAMPTZ NOT NULL,
    restaurant_id TEXT NOT NULL,
    cart_id TEXT NOT NULL,
    PRIMARY KEY(user_id, order_id)
);
INSERT INTO journeypulse.schema_migrations(version, description)
VALUES (2, 'Customer attributes, behavioral summaries and replay-safe receipts')
ON CONFLICT (version) DO NOTHING;
