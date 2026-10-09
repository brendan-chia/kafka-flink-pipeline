CREATE TABLE IF NOT EXISTS lakehouse.analytics.revenue_reconciled (
    window_start TIMESTAMP(3), window_end TIMESTAMP(3), currency VARCHAR,
    payment_count BIGINT, revenue DECIMAL(38,2), source_snapshot_id BIGINT,
    reconciled_at TIMESTAMP(3)
) WITH (format='PARQUET', format_version=2, partitioning=ARRAY['day(window_start)']);
