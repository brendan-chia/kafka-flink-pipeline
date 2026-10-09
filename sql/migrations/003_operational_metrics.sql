-- Repeatable, non-destructive support for recent audit latency queries.
BEGIN;
CREATE INDEX IF NOT EXISTS idx_processed_at ON processed_events(processed_at);
COMMIT;
