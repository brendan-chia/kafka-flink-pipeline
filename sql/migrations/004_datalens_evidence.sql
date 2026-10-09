-- DataLens evidence foundation. Repeatable and non-destructive.
BEGIN;
CREATE SCHEMA IF NOT EXISTS datalens;
CREATE TABLE IF NOT EXISTS datalens.datasets (
    name TEXT PRIMARY KEY, description TEXT NOT NULL, owner TEXT NOT NULL,
    physical_location TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS datalens.metrics (
    name TEXT PRIMARY KEY, definition TEXT NOT NULL, source_dataset TEXT NOT NULL
        REFERENCES datalens.datasets(name), semantics JSONB NOT NULL,
    definition_version INTEGER NOT NULL CHECK (definition_version > 0)
);
CREATE TABLE IF NOT EXISTS datalens.dependencies (
    upstream TEXT NOT NULL, downstream TEXT NOT NULL, description TEXT NOT NULL,
    PRIMARY KEY (upstream, downstream), CHECK (upstream <> downstream)
);
CREATE TABLE IF NOT EXISTS datalens.quality_results (
    result_id UUID PRIMARY KEY, observed_at TIMESTAMPTZ NOT NULL,
    check_name TEXT NOT NULL, dataset TEXT NOT NULL REFERENCES datalens.datasets(name),
    status TEXT NOT NULL CHECK (status IN ('observed','partial','unknown','match','mismatch')),
    range_start TIMESTAMPTZ NOT NULL, range_end TIMESTAMPTZ NOT NULL,
    evidence JSONB NOT NULL, CHECK (range_end >= range_start)
);
CREATE INDEX IF NOT EXISTS quality_results_lookup
    ON datalens.quality_results(dataset, check_name, observed_at DESC);
CREATE INDEX IF NOT EXISTS idx_processed_payment_event_time
    ON processed_events(event_timestamp_ms, currency) WHERE event_type = 'payment';
INSERT INTO datalens.datasets VALUES
('user-events','Kafka source deliveries; includes duplicates and invalid records','pipeline','kafka:user-events'),
('processed_events','Valid audit records, upserted by immutable event_id; includes late arrivals','pipeline','postgres:public.processed_events'),
('payment_revenue_windows','Finalized event-time payment aggregates by UTC window and currency','pipeline','postgres:public.payment_revenue_windows'),
('activity_windows','Finalized activity counts by UTC window, type and currency','pipeline','postgres:public.activity_windows'),
('user-events-dlq','Invalid payload deliveries with validation reasons; replay can duplicate deliveries','pipeline','kafka:user-events-dlq')
ON CONFLICT (name) DO NOTHING;
INSERT INTO datalens.metrics VALUES
('payment_revenue','Sum of valid payment event amounts; simulated gross payments, not recognized food-delivery revenue.',
'payment_revenue_windows',
'{"event_type":"payment","currency_policy":"separate currencies; no conversion","time_basis":"event_timestamp_ms","window_alignment":"UTC epoch","default_window_seconds":300,"default_watermark_seconds":10,"configuration_note":"defaults only; record actual configuration with evidence","late_policy":"audit retains late events; finalized windows require manual reconciliation","refunds":"not modelled","business_day_timezone":"Asia/Kuala_Lumpur","identity":"immutable event_id"}',1),
('activity_count','Count of distinct event IDs within a window, grouped by type and currency; food_order is not a completed-order lifecycle.',
'activity_windows','{"time_basis":"event_timestamp_ms","window_alignment":"UTC epoch","currency_policy":"separate currencies","late_policy":"manual reconciliation","identity":"immutable event_id"}',1)
ON CONFLICT (name) DO NOTHING;
INSERT INTO datalens.dependencies VALUES
('user-events','processed_events','Validation routes valid deliveries to the audit sink'),
('user-events','user-events-dlq','Validation routes invalid deliveries to the DLQ'),
('user-events','payment_revenue_windows','Flink validates, deduplicates within windows and aggregates payments'),
('user-events','activity_windows','Flink validates, deduplicates within windows and aggregates activity'),
('payment_revenue_windows','metric:payment_revenue','Business metric reads finalized payment windows'),
('activity_windows','metric:activity_count','Business metric reads finalized activity windows'),
('processed_events','diagnostic:revenue_comparison','Read-only audit aggregation supplies comparison evidence')
ON CONFLICT (upstream, downstream) DO NOTHING;
COMMIT;
