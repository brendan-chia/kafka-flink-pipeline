-- Apply once to an existing database BEFORE starting the upgraded pipeline.
-- No existing rows are deleted. Legacy rows cannot be reconciled to Kafka:
-- their old schema did not retain event identity or source coordinates.
BEGIN;
ALTER TABLE processed_events ADD COLUMN IF NOT EXISTS event_id VARCHAR(100);
ALTER TABLE processed_events ADD COLUMN IF NOT EXISTS currency CHAR(3);
ALTER TABLE processed_events ADD COLUMN IF NOT EXISTS event_timestamp_ms BIGINT;
ALTER TABLE processed_events ADD COLUMN IF NOT EXISTS ingested_at TIMESTAMP;
ALTER TABLE processed_events ADD COLUMN IF NOT EXISTS source_topic TEXT;
ALTER TABLE processed_events ADD COLUMN IF NOT EXISTS source_partition INTEGER;
ALTER TABLE processed_events ADD COLUMN IF NOT EXISTS source_offset BIGINT;
UPDATE processed_events SET event_id = 'legacy:' || id::text WHERE event_id IS NULL;
ALTER TABLE processed_events ALTER COLUMN event_id SET NOT NULL;
CREATE UNIQUE INDEX IF NOT EXISTS idx_processed_event_id ON processed_events(event_id);
-- New pipeline writes all metadata. Historical columns remain NULL rather
-- than inventing currency, event time, or Kafka provenance.
COMMIT;
