-- Run inside the bootstrap transaction. Existing public/DataLens tables are untouched.
CREATE SCHEMA IF NOT EXISTS journeypulse;
CREATE TABLE IF NOT EXISTS journeypulse.event_definitions (
    event_name TEXT NOT NULL,
    schema_version INTEGER NOT NULL CHECK (schema_version > 0),
    definition JSONB NOT NULL CHECK (jsonb_typeof(definition) = 'object'),
    registered_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (event_name, schema_version)
);
CREATE TABLE IF NOT EXISTS journeypulse.schema_migrations (
    version INTEGER PRIMARY KEY CHECK (version > 0),
    description TEXT NOT NULL,
    applied_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);
INSERT INTO journeypulse.schema_migrations(version, description)
VALUES (1, 'JourneyPulse contract catalogue foundation')
ON CONFLICT (version) DO NOTHING;
