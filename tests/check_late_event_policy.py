"""Verify late-data behavior using a live, monitored filesystem source.

Wait for a window result before delivering a late event, making the watermark
boundary observable rather than depending on sleeps or input ordering.
"""
import importlib.util
import json
import tempfile
import threading
import time
from decimal import Decimal
from pathlib import Path

path = Path(__file__).resolve().parents[1] / 'flink-processor' / 'pipeline.py'
spec = importlib.util.spec_from_file_location('pipeline', path)
pipeline = importlib.util.module_from_spec(spec)
spec.loader.exec_module(pipeline)
env = pipeline.create_table_env()
env.get_config().set('restart-strategy.type', 'none')
env.get_config().set('python.fn-execution.bundle.size', '1')
assert pipeline.WINDOW_SECONDS == 300, 'Run with the default window settings.'
base = 1790985600000
outputs = []
errors = []


def wait_for(predicate):
    deadline = time.monotonic() + 60
    while time.monotonic() < deadline:
        if errors:
            raise errors[0]
        if predicate():
            return
        time.sleep(0.1)
    raise AssertionError(f'Timed out waiting for Flink output: {outputs}')


with tempfile.TemporaryDirectory(prefix='late-events-') as directory:
    def publish(name, timestamp, event_type='food_order', amount=0):
        event = dict(event_id=name, user_id='test_user', event_type=event_type,
                     timestamp=timestamp, amount=amount, currency='MYR')
        row = {**event, 'raw_payload': json.dumps(event), 'error_reason': None}
        # Atomic publication keeps the continuous source from reading partial files.
        temporary = Path(directory) / f'.{name}.tmp'
        temporary.write_text(json.dumps(row) + '\n', encoding='utf-8')
        temporary.rename(Path(directory) / f'{name}.json')

    env.execute_sql(f'''
        CREATE TABLE validated_events (
            event_id STRING, event_type STRING, currency STRING, amount DECIMAL(10,2),
            raw_payload STRING, error_reason STRING,
            event_time AS TO_TIMESTAMP_LTZ(validated_timestamp(CAST(raw_payload AS BYTES)), 3),
            WATERMARK FOR event_time AS event_time - INTERVAL '10' SECOND
        ) WITH ('connector'='filesystem', 'path'='{Path(directory).as_uri()}',
                'format'='json', 'source.monitor-interval'='1 s')
    ''')
    pipeline.create_business_views(env)
    query = '''
        SELECT 'audit' AS tag, event_id, CAST(event_time AS TIMESTAMP(3)) AS window_start,
               currency, CAST(1 AS BIGINT) AS event_count, CAST(amount AS DECIMAL(38,2)) AS amount
        FROM validated_events
        UNION ALL
        SELECT 'revenue', '', window_start, currency, payment_count, revenue
        FROM (''' + pipeline.business_window_query(True) + ''')
    '''
    result = env.sql_query(query).execute()
    iterator = result.collect()

    def consume():
        try:
            for row in iterator:
                outputs.append(tuple(row))
        except Exception as error:
            errors.append(error)

    thread = threading.Thread(target=consume, daemon=True)
    thread.start()
    try:
        publish('on-time', base + 1000, 'payment', 10.25)
        wait_for(lambda: any(row[1] == 'on-time' for row in outputs))
        publish('advance-one', base + 320000)
        wait_for(lambda: any(row[0] == 'revenue' for row in outputs))
        # Window closure has now been observed, so this event is definitively late.
        publish('late-payment', base + 2000, 'payment', 99)
        wait_for(lambda: any(row[1] == 'late-payment' for row in outputs))
        publish('next-window-payment', base + 330000, 'payment', 2)
        wait_for(lambda: any(row[1] == 'next-window-payment' for row in outputs))
        publish('advance-two', base + 620000)
        wait_for(lambda: len([row for row in outputs if row[0] == 'revenue']) >= 2)
        revenue = [row for row in outputs if row[0] == 'revenue']
        assert sorted((row[4], row[5]) for row in revenue) == [
            (1, Decimal('2.00')), (1, Decimal('10.25'))], revenue
        print('PASS: closed window excludes late payment; audit stream retains it; next window closes normally.')
    finally:
        result.get_job_client().cancel().result()
        iterator.close()
        thread.join(timeout=5)
