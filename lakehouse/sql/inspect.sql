-- Run individual statements through Trino's CLI or scripts/lakehouse.py query.
SELECT * FROM lakehouse.analytics."event_history$history" ORDER BY made_current_at DESC;
SELECT * FROM lakehouse.analytics."validated_events$snapshots" ORDER BY committed_at DESC;
SELECT file_path, record_count, file_size_in_bytes FROM lakehouse.analytics."validated_events$files";
SELECT error_reason, count(*) FROM lakehouse.analytics.event_history GROUP BY error_reason;
-- Final streaming emissions; fresh Kafka replay can append additional versions.
SELECT * FROM lakehouse.analytics.revenue_finalized ORDER BY window_start DESC;
-- Explicitly repaired late-inclusive batch results.
SELECT * FROM lakehouse.analytics.revenue_reconciled ORDER BY window_start DESC;
