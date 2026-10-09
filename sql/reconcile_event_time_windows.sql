-- Recompute completed UTC windows from unique audit rows, including late data.
-- Stop the Flink job first. This script does not delete source or output rows.
-- Required psql variables: from_utc, until_utc. Optional: window_seconds (300).
\if :{?window_seconds}
\else
\set window_seconds 300
\endif
BEGIN ISOLATION LEVEL REPEATABLE READ;
CREATE TEMP TABLE reconcile_settings (
    window_seconds INTEGER CHECK (window_seconds BETWEEN 1 AND 86400),
    from_utc TIMESTAMP NOT NULL,
    until_utc TIMESTAMP NOT NULL,
    CHECK (until_utc > from_utc),
    CHECK (mod(extract(epoch FROM from_utc), window_seconds) = 0),
    CHECK (mod(extract(epoch FROM until_utc), window_seconds) = 0)
) ON COMMIT DROP;
INSERT INTO reconcile_settings VALUES
    (:'window_seconds'::integer, :'from_utc'::timestamp, :'until_utc'::timestamp);
CREATE TEMP TABLE reconciled_activity ON COMMIT DROP AS
WITH bucketed AS (
    SELECT floor(p.event_timestamp_ms::numeric / (s.window_seconds * 1000::bigint))
               * (s.window_seconds * 1000::bigint) AS start_ms,
           s.window_seconds, p.event_type, p.currency, p.amount
    FROM processed_events p CROSS JOIN reconcile_settings s
    WHERE p.event_timestamp_ms >= extract(epoch FROM s.from_utc) * 1000
      AND p.event_timestamp_ms < extract(epoch FROM s.until_utc) * 1000
      AND p.currency IS NOT NULL
)
SELECT timezone('UTC', to_timestamp(start_ms / 1000.0)) AS window_start,
       timezone('UTC', to_timestamp(start_ms / 1000.0 + window_seconds)) AS window_end,
       event_type, currency, count(*)::bigint AS event_count,
       sum(amount)::numeric(38,2) AS total_amount
FROM bucketed GROUP BY start_ms, window_seconds, event_type, currency;
INSERT INTO payment_revenue_windows
SELECT window_start, window_end, currency, event_count, total_amount
FROM reconciled_activity WHERE event_type = 'payment'
ON CONFLICT (window_start, window_end, currency) DO UPDATE
SET payment_count = EXCLUDED.payment_count, revenue = EXCLUDED.revenue;
INSERT INTO activity_windows
SELECT window_start, window_end, event_type, currency, event_count
FROM reconciled_activity
ON CONFLICT (window_start, window_end, event_type, currency) DO UPDATE
SET event_count = EXCLUDED.event_count;
COMMIT;
