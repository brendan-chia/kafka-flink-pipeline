"""Persist scoped observations; compare revenue without modifying business tables."""
from datetime import datetime, timezone
from decimal import Decimal
import os
import re
from uuid import uuid4

import psycopg2
from psycopg2.extras import Json, RealDictCursor

UTC = timezone.utc


def json_value(value):
    if isinstance(value, Decimal):
        return str(value)  # Preserve exact money in JSON.
    if isinstance(value, datetime):
        return value.isoformat()
    raise TypeError(type(value).__name__)


def connection_params():
    return dict(host=os.getenv('POSTGRES_HOST', 'localhost'),
                port=int(os.getenv('POSTGRES_PORT', '5432')),
                dbname=os.getenv('POSTGRES_DB', 'grabevents'),
                user=os.getenv('POSTGRES_USER', 'grabuser'),
                password=os.getenv('POSTGRES_PASS', 'grabpass'))


def connect(params, readonly=True):
    conn = psycopg2.connect(**dict(params, connect_timeout=5,
        options='-c statement_timeout=5000 -c timezone=UTC'))
    try:
        conn.set_session(readonly=readonly, isolation_level='REPEATABLE READ')
        return conn
    except Exception:
        conn.close()
        raise


def persist_result(params, check_name, dataset, status, start, end, evidence):
    """Only this writer modifies the evidence schema; never repairs pipeline output."""
    conn = connect(params, readonly=False)
    result_id = str(uuid4())
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute('''INSERT INTO datalens.quality_results
                    (result_id,observed_at,check_name,dataset,status,range_start,range_end,evidence)
                    VALUES (%s,CURRENT_TIMESTAMP,%s,%s,%s,%s,%s,%s)''',
                    (result_id, check_name, dataset, status, start, end, Json(evidence)))
        return result_id
    finally:
        conn.close()


FRESHNESS_SQL = '''SELECT CURRENT_TIMESTAMP AS observed_at,
    count(*) AS payment_count, max(processed_at) AS latest_processed_at,
    max(ingested_at) AS latest_ingested_at,
    max(event_timestamp_ms) AS latest_event_timestamp_ms,
    EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP AT TIME ZONE 'UTC' - max(processed_at))) AS processing_age_seconds,
    EXTRACT(EPOCH FROM CURRENT_TIMESTAMP) - max(event_timestamp_ms) / 1000.0 AS event_age_seconds,
    count(*) FILTER (WHERE processed_at >= CURRENT_TIMESTAMP AT TIME ZONE 'UTC' - INTERVAL '5 minutes') AS recent_payment_count
    FROM processed_events WHERE event_type = 'payment' '''


def payment_freshness(params):
    conn = connect(params)
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(FRESHNESS_SQL)
            return dict(cur.fetchone())
    finally:
        conn.close()


def record_payment_freshness(params, row=None):
    row = payment_freshness(params) if row is None else row
    now = row['observed_at']
    # Audit freshness alone cannot establish source completeness or an outage.
    evidence = {key: (json_value(value) if isinstance(value, (datetime, Decimal)) else value)
                for key, value in row.items()}
    evidence['limitations'] = 'Unique audit rows only; no proof of source completeness. Replay may preserve old processing timestamps.'
    return persist_result(params, 'payment_freshness', 'processed_events',
        'observed' if row['payment_count'] else 'unknown', now, now, evidence)


def record_source_quality(params, valid, invalid, reasons, now, coverage_start, config):
    """Snapshot an observer window, explicitly retaining restart/coverage limitations."""
    start = now - 300
    if coverage_start > now:
        raise ValueError('Observer coverage begins in the future')
    if valid < 0 or invalid < 0 or sum(reasons.values()) != invalid:
        raise ValueError('Inconsistent source quality counts')
    status = 'partial' if coverage_start > now - 300 else ('observed' if valid + invalid else 'unknown')
    return persist_result(params, 'source_validation_window', 'user-events', status,
        datetime.fromtimestamp(start, UTC), datetime.fromtimestamp(now, UTC),
        dict(valid_deliveries=valid, invalid_deliveries=invalid,
             invalid_fraction=invalid / (valid + invalid) if valid + invalid else None,
             reasons=dict(reasons), coverage_start=datetime.fromtimestamp(coverage_start, UTC).isoformat(),
             config=config, limitations='Latest-offset observer; counts deliveries including duplicates. Polling backlog may leave coverage incomplete. Not a historical source census.'))


def validate_range(start, end, currency, seconds, now=None):
    if not isinstance(seconds, int) or isinstance(seconds, bool) or not 1 <= seconds <= 86400:
        raise ValueError('Window seconds must be an integer in 1..86400')
    if not re.fullmatch(r'[A-Z]{3}', currency):
        raise ValueError('Currency must be three uppercase letters')
    values = []
    for value in (start, end):
        dt = datetime.fromisoformat(value.replace('Z', '+00:00'))
        if dt.tzinfo is None or dt.utcoffset().total_seconds() != 0:
            raise ValueError('Use explicit UTC timestamps ending in Z or +00:00')
        if dt.microsecond or int(dt.timestamp()) % seconds:
            raise ValueError('Range boundaries must align to the configured window')
        values.append(dt)
    now = now or datetime.now(UTC)
    if values[0] >= values[1] or values[1] > now:
        raise ValueError('Choose an increasing range ending in the past')
    if (values[1] - values[0]).total_seconds() > 7 * 86400:
        raise ValueError('Comparison range cannot exceed seven days')
    return values


COMPARISON_SQL = '''WITH expected AS (
    SELECT timezone('UTC', to_timestamp(floor(event_timestamp_ms::numeric / (%s * 1000::bigint)) * %s)) AS window_start,
           count(*) AS audit_count, sum(amount) AS audit_revenue
    FROM processed_events WHERE event_type = 'payment' AND currency = %s
      AND event_timestamp_ms >= %s AND event_timestamp_ms < %s
    GROUP BY 1
), stored AS (
    SELECT window_start, window_end, payment_count, revenue
    FROM payment_revenue_windows WHERE currency = %s
      AND window_start >= %s AND window_start < %s
)
SELECT coalesce(e.window_start,s.window_start) AS window_start, s.window_end,
       e.audit_count, e.audit_revenue, s.payment_count AS stored_count, s.revenue AS stored_revenue
FROM expected e FULL OUTER JOIN stored s ON e.window_start = s.window_start
ORDER BY 1,2 LIMIT 10001'''


def compare_revenue(params, start, end, currency='MYR', seconds=300, *, connection=None):
    start_dt, end_dt = validate_range(start, end, currency, seconds)
    conn = connection if connection is not None else connect(params)
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute('SELECT CURRENT_TIMESTAMP AS observed_at')
            observed_at = cur.fetchone()['observed_at']
            cur.execute(COMPARISON_SQL, (seconds, seconds, currency,
                int(start_dt.timestamp()) * 1000, int(end_dt.timestamp()) * 1000,
                currency, start_dt.replace(tzinfo=None), end_dt.replace(tzinfo=None)))
            rows = [dict(row) for row in cur.fetchall()]
        if len(rows) > 10000:
            raise ValueError('Comparison exceeds 10000 rows; narrow the range')
        for row in rows:
            if row['window_end'] is not None and (row['window_end'] - row['window_start']).total_seconds() != seconds:
                row['finding'] = 'incompatible_window_configuration'
            elif row['stored_count'] is None:
                row['finding'] = 'missing_aggregate'
            elif row['audit_count'] is None:
                row['finding'] = 'aggregate_without_audit'
            elif row['audit_count'] != row['stored_count'] or row['audit_revenue'] != row['stored_revenue']:
                row['finding'] = 'mismatch'
            else:
                row['finding'] = 'match'
            row['revenue_delta'] = (row['audit_revenue'] or Decimal(0)) - (row['stored_revenue'] or Decimal(0))
        return dict(status=('no_data' if not rows else 'match' if all(r['finding'] == 'match' for r in rows) else 'mismatch'),
            observed_at=observed_at, from_utc=start_dt, until_utc=end_dt, currency=currency,
            window_seconds=seconds, rows=rows,
            limitations='Consistent PostgreSQL snapshot; audit is not proof of source completeness. Discrepancies do not establish cause. Missing audit IDs and mutated event IDs cannot be reconstructed.')
    finally:
        if connection is None:
            conn.close()
