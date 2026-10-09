"""Execute real Flink windows against shuffled, duplicated, bounded fixtures."""
import importlib.util
import json
import random
import tempfile
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path

path = Path(__file__).resolve().parents[1] / 'flink-processor' / 'pipeline.py'
spec = importlib.util.spec_from_file_location('pipeline', path)
pipeline = importlib.util.module_from_spec(spec)
spec.loader.exec_module(pipeline)
from event_contract import decode_event
env = pipeline.create_table_env()
env.get_config().set('restart-strategy.type', 'none')
base = 1790985600000  # 2026-10-03 00:00:00 UTC, aligned to five minutes.
assert pipeline.WINDOW_SECONDS == 300, 'Run this regression check with the default 300-second window.'
events = [
    dict(event_id='a', event_type='payment', timestamp=base + 10000, amount=10.10, currency='MYR'),
    dict(event_id='b', event_type='payment', timestamp=base + 2000, amount=0.20, currency='MYR'),
    dict(event_id='c', event_type='food_order', timestamp=base + 120000, amount=20.00, currency='MYR'),
    dict(event_id='d', event_type='payment', timestamp=base + 40000, amount=3.33, currency='USD'),
    dict(event_id='e', event_type='payment', timestamp=base + 300000, amount=5.00, currency='MYR'),
]
for event in events:
    event['user_id'] = 'test_user'
fixtures = events + [events[0].copy(), dict(events[0], event_id='invalid',
                                         user_id=None, timestamp=253402300799999),
                     dict(events[0], event_id='future', timestamp=253402300799999)]
random.Random(42).shuffle(fixtures)


def collect(query):
    with env.sql_query(query).execute().collect() as rows:
        return [tuple(row) for row in rows]


with tempfile.TemporaryDirectory(prefix='event-time-') as directory:
    file = Path(directory) / 'events.json'
    records = []
    for event in fixtures:
        # Invalid raw records are excluded, matching the validation branch.
        payload = json.dumps(event)
        reason = decode_event(payload.encode(), base + 900000)[7]
        records.append(json.dumps({**event, 'raw_payload': payload,
                                  'received_timestamp_ms': base + 900000,
                                  'error_reason': reason}))
    file.write_text('\n'.join(records) + '\n', encoding='utf-8')
    env.execute_sql(f'''
        CREATE TABLE validated_events (
            event_id STRING, event_type STRING, currency STRING, amount DECIMAL(10,2),
            raw_payload STRING, error_reason STRING, received_timestamp_ms BIGINT,
            event_time AS TO_TIMESTAMP_LTZ(validated_timestamp(CAST(raw_payload AS BYTES),
                                           TO_TIMESTAMP_LTZ(received_timestamp_ms, 3)), 3),
            WATERMARK FOR event_time AS event_time - INTERVAL '10' SECOND
        ) WITH ('connector'='filesystem', 'path'='{Path(directory).as_uri()}', 'format'='json')
    ''')
    pipeline.create_business_views(env)
    revenue = collect(pipeline.business_window_query(True))
    activity = collect(pipeline.business_window_query())

# Independent batch oracle: group immutable event IDs by UTC epoch window.
expected_revenue = {}
expected_activity = {}
for event in {event['event_id']: event for event in events}.values():
    start_ms = event['timestamp'] // 300000 * 300000
    start = datetime.fromtimestamp(start_ms / 1000, timezone.utc).replace(tzinfo=None)
    end = datetime.fromtimestamp((start_ms + 300000) / 1000, timezone.utc).replace(tzinfo=None)
    key = (start, end, event['event_type'], event['currency'])
    expected_activity[key] = expected_activity.get(key, 0) + 1
    if event['event_type'] == 'payment':
        key = (start, end, event['currency'])
        count, total = expected_revenue.get(key, (0, Decimal('0.00')))
        expected_revenue[key] = (count + 1, total + Decimal(str(event['amount'])))
assert {row[:3]: row[3:] for row in revenue} == expected_revenue, revenue
assert {row[:4]: row[4] for row in activity} == expected_activity, activity
print('PASS: Flink event-time revenue/activity match batch oracle after shuffle and duplicate injection.')
print('PASS: currency separation, exact decimals, window boundaries and invalid future timestamps.')
