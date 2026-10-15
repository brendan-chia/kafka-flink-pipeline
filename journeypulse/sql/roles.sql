-- Optional administrator-run privileges; not part of bootstrap migrations.
-- Grant these NOLOGIN roles to dedicated login roles with separate passwords.
DO $$ BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname='journeypulse_processor') THEN
        CREATE ROLE journeypulse_processor NOLOGIN;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname='journeypulse_serving') THEN
        CREATE ROLE journeypulse_serving NOLOGIN;
    END IF;
END $$;
GRANT USAGE ON SCHEMA journeypulse TO journeypulse_processor, journeypulse_serving;
GRANT SELECT ON journeypulse.schema_migrations TO journeypulse_processor, journeypulse_serving;
GRANT SELECT(user_id), INSERT(user_id) ON journeypulse.customers TO journeypulse_processor;
GRANT SELECT, INSERT, UPDATE ON journeypulse.behavioral_summaries TO journeypulse_processor;
GRANT SELECT, INSERT, UPDATE ON journeypulse.kafka_receipts TO journeypulse_processor;
GRANT SELECT, INSERT, DELETE ON journeypulse.event_ids TO journeypulse_processor;
GRANT SELECT, INSERT ON journeypulse.completed_orders TO journeypulse_processor;
GRANT SELECT ON journeypulse.customers, journeypulse.behavioral_summaries TO journeypulse_serving;
GRANT INSERT(user_id) ON journeypulse.customers TO journeypulse_serving;
GRANT UPDATE(preferred_language, marketing_consent, consent_updated_at,
             onboarded_at, attributes_updated_at) ON journeypulse.customers TO journeypulse_serving;
GRANT SELECT ON journeypulse.segment_definitions TO journeypulse_processor, journeypulse_serving;
GRANT SELECT, INSERT, UPDATE ON journeypulse.carts, journeypulse.segment_evaluations,
    journeypulse.segment_memberships TO journeypulse_processor;
GRANT SELECT, INSERT, DELETE ON journeypulse.segment_views TO journeypulse_processor;
GRANT SELECT ON journeypulse.segment_evaluations, journeypulse.segment_memberships TO journeypulse_serving;
