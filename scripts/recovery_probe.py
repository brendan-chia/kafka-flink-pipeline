"""Publish a finite fixture set, then reconcile it before/after a pipeline restart.

Uses the same environment settings as pipeline.py. Never clears topics or tables.
"""
import argparse
import base64
import json
import os
from pathlib import Path
import time
from uuid import uuid4

from kafka import KafkaConsumer, KafkaProducer, TopicPartition
import psycopg2


def publish(path):
    if path.exists():
        raise SystemExit('Manifest already exists. Use --verify or choose another --manifest.')
    run_id = str(uuid4())
    events = [{'event_id': f'{run_id}-{i}', 'user_id': 'recovery_probe',
               'event_type': 'payment', 'timestamp': time.time_ns() // 1_000_000,
               'amount': i + 0.25, 'currency': 'MYR'} for i in range(3)]
    valid = [json.dumps(event).encode() for event in events]
    invalid = [b'{broken', json.dumps(dict(events[0], amount=None)).encode(), b'\xff', None]
    manifest = {'run_id': run_id, 'valid': events, 'invalid': []}
    path.parent.mkdir(parents=True, exist_ok=True)
    producer = KafkaProducer(bootstrap_servers=os.getenv('KAFKA_BOOTSTRAP', 'localhost:29092'),
                             acks='all', retries=3, max_in_flight_requests_per_connection=1)
    try:
        for payload in valid + [valid[0]]:
            producer.send(os.getenv('SOURCE_TOPIC', 'user-events'),
                          key=run_id.encode(), value=payload).get(timeout=30)
        for payload in invalid:
            metadata = producer.send(os.getenv('SOURCE_TOPIC', 'user-events'),
                                     key=run_id.encode(), value=payload).get(timeout=30)
            manifest['invalid'].append({
                'record_id': f'{metadata.topic}:{metadata.partition}:{metadata.offset}',
                'raw_payload_base64': base64.b64encode(payload or b'').decode(),
            })
    finally:
        producer.close()
    path.write_text(json.dumps(manifest, indent=2), encoding='utf-8')
    print(f'Published 3 valid event IDs, 1 duplicate and 4 invalid records. Manifest: {path}')


def verify(path, timeout, min_dlq_deliveries=1):
    manifest = json.loads(path.read_text(encoding='utf-8'))
    expected_valid = {event['event_id']: event for event in manifest['valid']}
    expected_dlq = {event['record_id']: event for event in manifest['invalid']}
    consumer = KafkaConsumer(bootstrap_servers=os.getenv('KAFKA_BOOTSTRAP', 'localhost:29092'),
                             enable_auto_commit=False, group_id=None)
    conn = psycopg2.connect(host=os.getenv('POSTGRES_HOST', 'localhost'),
                           port=int(os.getenv('POSTGRES_PORT', '5432')),
                           dbname=os.getenv('POSTGRES_DB', 'grabevents'),
                           user=os.getenv('POSTGRES_USER', 'grabuser'),
                           password=os.getenv('POSTGRES_PASS', 'grabpass'))
    conn.autocommit = True
    seen_dlq = set()
    dlq_deliveries = {record_id: 0 for record_id in expected_dlq}
    seen_valid = set()
    deadline = time.monotonic() + timeout
    try:
        topic = os.getenv('DLQ_TOPIC', 'user-events-dlq')
        partitions = consumer.partitions_for_topic(topic)
        if not partitions:
            raise RuntimeError('DLQ topic is unavailable; start the pipeline first.')
        consumer.assign([TopicPartition(topic, p) for p in partitions])
        consumer.seek_to_beginning()
        while time.monotonic() < deadline:
            with conn.cursor() as cur:
                cur.execute('''SELECT event_id, amount, currency, event_timestamp_ms,
                                      COUNT(*) OVER (PARTITION BY event_id)
                               FROM processed_events WHERE event_id = ANY(%s)''',
                            (list(expected_valid),))
                for event_id, amount, currency, timestamp, count in cur.fetchall():
                    event = expected_valid[event_id]
                    assert count == 1, f'Duplicate PostgreSQL rows: {event_id}'
                    assert str(amount) == f"{event['amount']:.2f}", (event_id, amount)
                    assert currency == event['currency'] and timestamp == event['timestamp'], event_id
                    seen_valid.add(event_id)
            for records in consumer.poll(timeout_ms=500).values():
                for record in records:
                    if record.value is None:
                        continue
                    event = json.loads(record.value)
                    record_id = event.get('record_id')
                    if record_id in expected_dlq:
                        assert event['raw_payload_base64'] == expected_dlq[record_id]['raw_payload_base64'], record_id
                        assert event['error_reason'] and json.loads(event['validation_errors']), record_id
                        seen_dlq.add(record_id)
                        dlq_deliveries[record_id] += 1
            if (seen_valid == set(expected_valid) and seen_dlq == set(expected_dlq)
                    and all(count >= min_dlq_deliveries for count in dlq_deliveries.values())):
                print('PASS: 3 unique PostgreSQL rows and all 4 lossless DLQ records reconciled.')
                return {'valid_event_count': len(seen_valid), 'invalid_record_count': len(seen_dlq),
                        'dlq_deliveries': dlq_deliveries,
                        'missing_event_ids': [], 'missing_dlq_record_ids': []}
        raise AssertionError(f'Missing valid IDs: {set(expected_valid) - seen_valid}; '
                             f'missing DLQ IDs: {set(expected_dlq) - seen_dlq}; '
                             f'DLQ deliveries: {dlq_deliveries}')
    finally:
        consumer.close()
        conn.close()


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument('--publish', action='store_true')
    mode.add_argument('--verify', action='store_true')
    parser.add_argument('--manifest', type=Path, default=Path('.flink-state/recovery-probe.json'))
    parser.add_argument('--timeout', type=float, default=60)
    args = parser.parse_args()
    if args.publish:
        publish(args.manifest)
    else:
        verify(args.manifest, args.timeout)
