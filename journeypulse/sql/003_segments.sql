-- Additive upgrade: pause JourneyPulse writers while bootstrapping and deploying.
CREATE TABLE IF NOT EXISTS journeypulse.segment_definitions (
    segment TEXT NOT NULL CHECK (segment IN ('active_browser','abandoned_cart','repeat_customer')),
    rule_version TEXT NOT NULL,
    definition JSONB NOT NULL,
    registered_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY(segment,rule_version)
);
CREATE TABLE IF NOT EXISTS journeypulse.carts (
    user_id TEXT NOT NULL REFERENCES journeypulse.customers(user_id),
    cart_id TEXT NOT NULL,
    latest_add_at TIMESTAMPTZ,
    latest_checkout_at TIMESTAMPTZ,
    completed_at TIMESTAMPTZ,
    PRIMARY KEY(user_id,cart_id)
);
-- Completed carts are terminal, including completion received before the add event.
INSERT INTO journeypulse.carts(user_id,cart_id,completed_at)
SELECT user_id,cart_id,min(occurred_at) FROM journeypulse.completed_orders GROUP BY user_id,cart_id
ON CONFLICT (user_id,cart_id) DO NOTHING;
CREATE TABLE IF NOT EXISTS journeypulse.segment_views (
    user_id TEXT NOT NULL REFERENCES journeypulse.customers(user_id),
    event_id TEXT PRIMARY KEY,
    occurred_at TIMESTAMPTZ NOT NULL
);
CREATE INDEX IF NOT EXISTS segment_views_customer_time ON journeypulse.segment_views(user_id,occurred_at);
CREATE TABLE IF NOT EXISTS journeypulse.segment_evaluations (
    user_id TEXT PRIMARY KEY REFERENCES journeypulse.customers(user_id),
    rule_version TEXT NOT NULL,
    evaluated_at TIMESTAMPTZ NOT NULL,
    next_evaluation_at TIMESTAMPTZ NOT NULL
);
CREATE INDEX IF NOT EXISTS segment_evaluations_due ON journeypulse.segment_evaluations(next_evaluation_at);
CREATE TABLE IF NOT EXISTS journeypulse.segment_memberships (
    user_id TEXT NOT NULL REFERENCES journeypulse.customers(user_id),
    segment TEXT NOT NULL,
    rule_version TEXT NOT NULL,
    member BOOLEAN NOT NULL,
    reason TEXT NOT NULL,
    evidence JSONB NOT NULL,
    evaluated_at TIMESTAMPTZ NOT NULL,
    valid_until TIMESTAMPTZ,
    PRIMARY KEY(user_id,segment,rule_version),
    FOREIGN KEY(segment,rule_version) REFERENCES journeypulse.segment_definitions(segment,rule_version)
);
INSERT INTO journeypulse.schema_migrations(version,description)
VALUES (3,'Versioned segments, terminal cart identities and durable scheduling') ON CONFLICT DO NOTHING;
