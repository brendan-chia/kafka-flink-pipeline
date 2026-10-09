"""Trino queries for snapshot-pinned replay and late-inclusive revenue repair."""
from datetime import datetime, timezone
import json
import time
import urllib.parse
import urllib.request

import lakehouse_config


class TrinoClient:
    def __init__(self, url='http://localhost:18080', timeout=120):
        self.url = url.rstrip('/')
        self.timeout = timeout

    def request(self, url, method='GET', data=None):
        # Do not follow a query continuation to an unrelated endpoint.
        if urllib.parse.urlsplit(url).netloc != urllib.parse.urlsplit(self.url).netloc:
            raise RuntimeError('Trino continuation changed origin')
        request = urllib.request.Request(url, data=data, method=method,
            headers={'X-Trino-User': 'lakehouse-local', 'X-Trino-Time-Zone': 'UTC',
                     'Content-Type': 'text/plain; charset=utf-8'})
        with urllib.request.urlopen(request, timeout=min(30, self.timeout)) as response:
            body = response.read()
        return json.loads(body) if body else {}

    def query(self, sql, max_rows=10000):
        result = self.request(self.url + '/v1/statement', 'POST', sql.encode())
        rows, columns = [], []
        deadline = time.monotonic() + self.timeout
        next_uri = None
        try:
            while True:
                next_uri = result.get('nextUri')
                if 'error' in result:
                    raise RuntimeError(result['error'].get('message', str(result['error'])))
                columns = result.get('columns', columns)
                rows.extend(result.get('data', []))
                if len(rows) > max_rows:
                    raise RuntimeError(f'Query exceeded {max_rows} rows; narrow the range')
                if not next_uri:
                    return [dict(zip((c['name'] for c in columns), row)) for row in rows]
                if time.monotonic() >= deadline:
                    raise TimeoutError('Trino query timed out')
                result = self.request(next_uri)
        except BaseException:
            if next_uri:
                try:
                    self.request(next_uri, 'DELETE')
                except Exception:
                    pass
            raise

    def current_snapshot(self, table):
        if table not in ('event_history', 'validated_events', 'revenue_finalized', 'revenue_reconciled'):
            raise ValueError('Unknown lakehouse table')
        rows = self.query(f'''SELECT snapshot_id FROM lakehouse.{lakehouse_config.namespace()}."{table}$history"
            WHERE is_current_ancestor ORDER BY made_current_at DESC LIMIT 1''')
        if not rows:
            raise ValueError(f'{table} has no committed snapshot yet')
        return int(rows[0]['snapshot_id'])


def validated_range(start, end, seconds=300):
    if not 1 <= seconds <= 86400:
        raise ValueError('Window seconds must be 1..86400')
    values = []
    for text in (start, end):
        timestamp = datetime.fromisoformat(text.replace('Z', '+00:00'))
        if timestamp.tzinfo is None or timestamp.utcoffset().total_seconds() != 0:
            raise ValueError('Use explicit UTC timestamps ending in Z or +00:00')
        ms = int(timestamp.timestamp() * 1000)
        if timestamp.microsecond or ms % (seconds * 1000):
            raise ValueError('Range boundaries must align to the configured window')
        values.append(ms)
    if values[0] >= values[1] or values[1] > int(datetime.now(timezone.utc).timestamp() * 1000):
        raise ValueError('Range must be increasing and end in the past')
    return tuple(values)


def canonical_cte(snapshot):
    snapshot = int(snapshot)
    if snapshot <= 0:
        raise ValueError('Snapshot ID must be positive')
    return f'''WITH deliveries AS (
        SELECT * FROM lakehouse.{lakehouse_config.namespace()}.validated_events FOR VERSION AS OF {snapshot}
    ), conflicts AS (
        SELECT event_id FROM deliveries GROUP BY event_id
        HAVING count(DISTINCT ROW(user_id,event_type,event_timestamp_ms,amount,currency)) > 1
    ), ranked AS (
        SELECT *, row_number() OVER (PARTITION BY event_id
            ORDER BY source_topic,source_partition,source_offset,archived_at) AS rn
        FROM deliveries WHERE event_id NOT IN (SELECT event_id FROM conflicts)
    ), canonical AS (SELECT * FROM ranked WHERE rn=1)'''


def reconciliation_sql(snapshot, start_ms, end_ms, seconds):
    """One atomic MERGE repairs a completed range, including stale group deletion."""
    prefix = f'lakehouse.{lakehouse_config.namespace()}'
    return f'''MERGE INTO {prefix}.revenue_reconciled target USING (
        {canonical_cte(snapshot)}, rebuilt AS (
            SELECT event_timestamp_ms - mod(event_timestamp_ms,{seconds * 1000}) AS start_ms,
                currency, count(*) AS payment_count, CAST(sum(amount) AS DECIMAL(38,2)) AS revenue
            FROM canonical WHERE event_type='payment'
                AND event_timestamp_ms >= {start_ms} AND event_timestamp_ms < {end_ms}
            GROUP BY 1,2
        ), desired AS (
            SELECT CAST(from_unixtime(start_ms/1000.0) AS timestamp(3)) AS window_start,
                CAST(from_unixtime(start_ms/1000.0 + {seconds}) AS timestamp(3)) AS window_end,
                currency, payment_count, revenue FROM rebuilt
        ), previous AS (
            SELECT * FROM {prefix}.revenue_reconciled
            WHERE window_start >= CAST(from_unixtime({start_ms}/1000.0) AS timestamp(3))
              AND window_start < CAST(from_unixtime({end_ms}/1000.0) AS timestamp(3))
        )
        SELECT coalesce(d.window_start,p.window_start) AS window_start,
            coalesce(d.window_end,p.window_end) AS window_end,
            coalesce(d.currency,p.currency) AS currency,d.payment_count,d.revenue
        FROM desired d FULL OUTER JOIN previous p
        ON d.window_start=p.window_start AND d.window_end=p.window_end AND d.currency=p.currency
    ) source ON target.window_start=source.window_start AND target.window_end=source.window_end
        AND target.currency=source.currency
    WHEN MATCHED AND source.payment_count IS NULL THEN DELETE
    WHEN MATCHED THEN UPDATE SET payment_count=source.payment_count,revenue=source.revenue,
        source_snapshot_id={int(snapshot)},reconciled_at=CAST(current_timestamp AS timestamp(3))
    WHEN NOT MATCHED AND source.payment_count IS NOT NULL THEN INSERT
        (window_start,window_end,currency,payment_count,revenue,source_snapshot_id,reconciled_at)
        VALUES (source.window_start,source.window_end,source.currency,source.payment_count,
                source.revenue,{int(snapshot)},CAST(current_timestamp AS timestamp(3)))'''


def replay_sql(snapshot, start_ms, end_ms, limit):
    if int(snapshot) <= 0 or int(limit) <= 0:
        raise ValueError('Snapshot and replay limit must be positive')
    return f'''SELECT record_id,source_topic,source_partition,source_offset,raw_payload_base64,payload_is_null
        FROM (SELECT *,row_number() OVER (PARTITION BY record_id ORDER BY archived_at) AS rn
            FROM lakehouse.{lakehouse_config.namespace()}.event_history FOR VERSION AS OF {int(snapshot)}
            WHERE ingested_at >= CAST(from_unixtime({int(start_ms)}/1000.0) AS timestamp(3))
              AND ingested_at < CAST(from_unixtime({int(end_ms)}/1000.0) AS timestamp(3)))
        WHERE rn=1 ORDER BY source_topic,source_partition,source_offset LIMIT {int(limit)+1}'''
