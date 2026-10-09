-- Apply after migration 001 to an existing database. Safe to rerun.
BEGIN;
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
COMMIT;
