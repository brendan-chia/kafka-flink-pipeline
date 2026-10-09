-- Stop all writers first. These are manual procedures, not startup commands.
-- Repeat for event_history, revenue_finalized and revenue_reconciled as needed.
ALTER TABLE lakehouse.analytics.validated_events EXECUTE optimize(file_size_threshold => '128MB');
ALTER TABLE lakehouse.analytics.validated_events EXECUTE optimize_manifests;
-- Only after all readers/replay/recovery jobs no longer need snapshots older than 7 days:
ALTER TABLE lakehouse.analytics.validated_events EXECUTE expire_snapshots(retention_threshold => '7d');
-- Only after stopping writers, and never with a horizon below the longest job/recovery interval:
ALTER TABLE lakehouse.analytics.validated_events EXECUTE remove_orphan_files(retention_threshold => '7d');
