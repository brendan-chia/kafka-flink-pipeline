"""Finite valid-traffic benchmark with broker acknowledgements and audit reconciliation.

Run against an already running pipeline. Never clears topics or database rows.
Visibility latency is measured by polling PostgreSQL, including JDBC buffering.
"""
import argparse
from datetime import datetime, timezone
from decimal import Decimal
import importlib.metadata
import json
import math
import os
from pathlib import Path
import platform
import random
import sys
import threading
import time
from uuid import uuid4

import psycopg2
from kafka import KafkaProducer


def percentile(values, q):
    if not values:
        return None
    ordered = sorted(values)
    index = (len(ordered) - 1) * q
    low = math.floor(index)
    high = math.ceil(index)
    return ordered[low] + (ordered[high] - ordered[low]) * (index - low)


def fixture(run_id, index, rng, timestamp_ms):
    return {'event_id': f'{run_id}-{index}', 'user_id': f'bench-{run_id}-{rng.randrange(128)}',
            'event_type': rng.choice(['food_order', 'ride_request', 'payment', 'grocery_order']),
            'timestamp': timestamp_ms, 'currency': 'MYR',
            'amount': rng.randint(1, 15000) / 100}


def reconcile(expected, rows):
    seen = set()
    duplicates, mismatches = [], []
    for event_id, amount, currency, timestamp, event_type in rows:
        if event_id not in expected:
            continue
        if event_id in seen:
            duplicates.append(event_id)
        seen.add(event_id)
        event = expected[event_id]
        if (amount != Decimal(str(event['amount'])) or currency != event['currency']
                or timestamp != event['timestamp'] or event_type != event['event_type']):
            mismatches.append(event_id)
    return {'missing_event_ids': sorted(set(expected) - seen),
            'duplicate_event_ids': sorted(set(duplicates)),
            'mismatched_event_ids': sorted(set(mismatches)), 'persisted_unique': len(seen)}


def pg_connect():
    return psycopg2.connect(host=os.getenv('POSTGRES_HOST', 'localhost'),
        port=int(os.getenv('POSTGRES_PORT', '5432')), dbname=os.getenv('POSTGRES_DB', 'grabevents'),
        user=os.getenv('POSTGRES_USER', 'grabuser'), password=os.getenv('POSTGRES_PASS', 'grabpass'),
        connect_timeout=5, options='-c statement_timeout=5000')


def run(args):
    run_id = str(uuid4())
    count = max(1, math.ceil(args.rate * args.duration))
    report = {'run_id': run_id, 'started_at_utc': datetime.now(timezone.utc).isoformat(),
              'status': 'dry_run' if args.dry_run else 'running',
              'workload': {'target_records_per_second': args.rate, 'records': count,
                           'seed': args.seed, 'profile': 'valid_unique', 'user_key_space': 128,
                           'source_topic': os.getenv('SOURCE_TOPIC', 'user-events'),
                           'declared_pipeline_parallelism': os.getenv('FLINK_PARALLELISM', '1')},
              'host': {'platform': platform.platform(), 'cpu': platform.processor(),
                       'logical_cpus': os.cpu_count(), 'python': sys.version,
                       'hardware_notes': args.hardware_notes},
              'versions': {name: importlib.metadata.version(name) for name in
                           ('apache-flink', 'kafka-python', 'psycopg2-binary')}}
    args.output.parent.mkdir(parents=True, exist_ok=True)
    # Refuse to overwrite previous evidence before publishing any records.
    with args.output.open('x', encoding='utf-8') as output:
        if args.dry_run:
            json.dump(report, output, indent=2)
            print(f'Dry run: {count} valid unique records; no network access. {args.output}')
            return True
        expected, started, visible, rows = {}, {}, {}, []
        stop = threading.Event()
        lock = threading.Lock()
        collector_errors = []
        conn = producer = None
        thread = None
        begin = time.monotonic()
        try:
            conn = pg_connect()
            conn.autocommit = True
            with conn.cursor() as cur:
                cur.execute('SHOW server_version')
                report['versions']['postgresql'] = cur.fetchone()[0]
            producer = KafkaProducer(bootstrap_servers=os.getenv('KAFKA_BOOTSTRAP', 'localhost:29092'),
                acks='all', retries=3, max_in_flight_requests_per_connection=1,
                value_serializer=lambda value: json.dumps(value).encode())

            def collect():
                nonlocal rows
                while not stop.is_set():
                    try:
                        with conn.cursor() as cur:
                            cur.execute('''SELECT event_id, amount, currency, event_timestamp_ms, event_type
                                FROM processed_events WHERE user_id = ANY(%s)''',
                                ([f'bench-{run_id}-{key}' for key in range(128)],))
                            current = cur.fetchall()
                        now = time.monotonic()
                        with lock:
                            rows = current
                            for row in current:
                                visible.setdefault(row[0], now)
                    except Exception as error:
                        collector_errors.append(str(error))
                    stop.wait(args.poll_interval)

            thread = threading.Thread(target=collect, daemon=True)
            thread.start()
            rng = random.Random(args.seed)
            begin = time.monotonic()
            pending = []

            def acknowledge(item):
                event, future = item
                metadata = future.get(timeout=30)
                with lock:
                    expected[event['event_id']] = event
                return {'event_id': event['event_id'], 'partition': metadata.partition, 'offset': metadata.offset}

            acknowledgements = []
            report['broker_acknowledgements'] = acknowledgements
            for index in range(count):
                delay = begin + index / args.rate - time.monotonic()
                if delay > 0:
                    time.sleep(delay)
                event = fixture(run_id, index, rng, time.time_ns() // 1_000_000)
                started[event['event_id']] = time.monotonic()
                pending.append((event, producer.send(report['workload']['source_topic'],
                    key=event['user_id'].encode(), value=event)))
                if len(pending) >= 64:
                    acknowledgements.append(acknowledge(pending.pop(0)))
            for item in pending:
                acknowledgements.append(acknowledge(item))
            publish_seconds = time.monotonic() - begin
            report['broker_acknowledgements'] = acknowledgements
            report['publish_seconds'] = publish_seconds
            report['acknowledged_records_per_second'] = len(expected) / publish_seconds
            deadline = time.monotonic() + args.timeout
            while time.monotonic() < deadline:
                with lock:
                    if set(expected) <= set(visible):
                        break
                time.sleep(args.poll_interval)
            with lock:
                result = reconcile(expected, rows)
                durations = [visible[event_id] - started[event_id] for event_id in expected if event_id in visible]
            report.update(result)
            report['acknowledged_records'] = len(expected)
            report['visibility_latency_seconds'] = {label: percentile(durations, q)
                for label, q in [('p50', 0.5), ('p95', 0.95), ('p99', 0.99)]}
            report['poll_interval_seconds'] = args.poll_interval
            report['reconciled_records_per_second'] = result['persisted_unique'] / (time.monotonic() - begin)
            report['collector_errors'] = collector_errors
            report['status'] = 'passed' if (len(expected) == count and not collector_errors and
                not any(result[name] for name in ('missing_event_ids', 'duplicate_event_ids', 'mismatched_event_ids'))) else 'failed'
        except Exception as error:
            report['status'] = 'failed'
            report['error'] = str(error)
            report['acknowledged_records'] = len(expected)
            report['acknowledged_events'] = list(expected.values())
        finally:
            stop.set()
            if thread is not None:
                thread.join(timeout=10)
            if producer is not None:
                try:
                    producer.close(timeout=10)
                except Exception as error:
                    report['status'] = 'failed'
                    report['producer_close_error'] = str(error)
            if conn is not None:
                conn.close()
            if report['status'] == 'running':
                report['status'] = 'interrupted'
            report['acknowledged_events'] = list(expected.values())
            json.dump(report, output, indent=2)
        print(f"{report['status'].upper()}: {args.output}")
        return report['status'] == 'passed'


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--rate', type=float, default=20)
    parser.add_argument('--duration', type=float, default=30)
    parser.add_argument('--timeout', type=float, default=120)
    parser.add_argument('--poll-interval', type=float, default=0.25)
    parser.add_argument('--seed', type=int, default=42)
    parser.add_argument('--hardware-notes', default='Not supplied; record RAM and Docker CPU/memory limits')
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--dry-run', action='store_true')
    args = parser.parse_args()
    if any(not math.isfinite(value) or value <= 0 for value in
           (args.rate, args.duration, args.timeout, args.poll_interval)):
        parser.error('Rate, duration, timeout and poll interval must be finite and positive')
    sys.exit(0 if run(args) else 1)
