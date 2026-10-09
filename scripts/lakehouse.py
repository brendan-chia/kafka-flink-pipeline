"""Snapshot inspection, atomic revenue reconciliation and lossless payload replay."""
import argparse
import base64
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import re
import sys
from uuid import uuid4

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import lakehouse_config
from lakehouse_ops import TrinoClient, canonical_cte, reconciliation_sql, replay_sql, validated_range

ROOT = Path(__file__).resolve().parents[1]


def bootstrap(client):
    sql = (ROOT / 'lakehouse/sql/bootstrap.sql').read_text().replace(
        'lakehouse.analytics.', f'lakehouse.{lakehouse_config.namespace()}.')
    client.query(sql.rstrip().rstrip(';'))


def reconcile(client, args):
    start, end = validated_range(args.from_utc, args.until_utc, args.window_seconds)
    snapshot = client.current_snapshot('validated_events') if args.snapshot is None else args.snapshot
    conflicts = client.query(canonical_cte(snapshot) + ' SELECT event_id FROM conflicts LIMIT 10')
    if conflicts:
        raise ValueError(f'Conflicting immutable event IDs; repair the contract violation first: {conflicts}')
    sql = reconciliation_sql(snapshot, start, end, args.window_seconds)
    if not args.execute:
        print(sql)
        return
    report = {'operation': 'reconcile', 'status': 'running', 'source_snapshot_id': snapshot,
              'from_utc': args.from_utc, 'until_utc': args.until_utc, 'window_seconds': args.window_seconds,
              'started_at_utc': datetime.now(timezone.utc).isoformat()}
    args.report.parent.mkdir(parents=True, exist_ok=True)
    with args.report.open('x', encoding='utf-8') as output:
        try:
            bootstrap(client)
            client.query(sql)
            report['status'] = 'committed'
            report['result_snapshot_id'] = client.current_snapshot('revenue_reconciled')
        except BaseException as error:
            # A client timeout does not prove that a submitted MERGE did not commit.
            report['status'] = 'unknown'
            report['error'] = str(error)
            raise
        finally:
            json.dump(report, output, indent=2)
    print(f'Atomic reconciliation committed from snapshot {snapshot}. Report: {args.report}')


def replay_payload(row):
    payload = base64.b64decode(row['raw_payload_base64'], validate=True)
    return None if row['payload_is_null'] else payload


def replay(client, args):
    start, end = validated_range(args.from_utc, args.until_utc, 1)
    snapshot = client.current_snapshot('event_history') if args.snapshot is None else args.snapshot
    records = client.query(replay_sql(snapshot, start, end, args.max_records),
                           max_rows=args.max_records + 1)
    if len(records) > args.max_records:
        raise ValueError('Replay range exceeds --max-records; narrow it before publishing')
    if not re.fullmatch(r'[a-zA-Z0-9][a-zA-Z0-9._-]{0,248}', args.target_topic):
        raise ValueError('Invalid Kafka target topic')
    if args.target_topic in {row['source_topic'] for row in records} | {os.getenv('SOURCE_TOPIC', 'user-events')}:
        raise ValueError('Replay target must differ from archived source topics and SOURCE_TOPIC')
    # Validate every stored payload before any send.
    payloads = [replay_payload(row) for row in records]
    report = {'operation': 'replay', 'status': 'preview', 'source_snapshot_id': snapshot,
              'target_topic': args.target_topic, 'records': len(records), 'acknowledged': [],
              'from_utc': args.from_utc, 'until_utc': args.until_utc}
    if not args.execute:
        print(json.dumps(report, indent=2))
        return
    from kafka import KafkaProducer
    args.report.parent.mkdir(parents=True, exist_ok=True)
    with args.report.open('x', encoding='utf-8') as output:
        producer = None
        try:
            report['status'] = 'publishing'
            producer = KafkaProducer(bootstrap_servers=os.getenv('KAFKA_BOOTSTRAP', 'localhost:29092'),
                                     acks='all', retries=3, max_in_flight_requests_per_connection=1)
            for row, payload in zip(records, payloads):
                metadata = producer.send(args.target_topic, key=row['record_id'].encode(), value=payload).get(timeout=30)
                report['acknowledged'].append({'original_record_id': row['record_id'],
                    'topic': metadata.topic, 'partition': metadata.partition, 'offset': metadata.offset})
                # Preserve acknowledged progress even if a later send fails.
                output.seek(0)
                json.dump(report, output, indent=2)
                output.truncate()
                output.flush()
            report['status'] = 'acknowledged'
        except BaseException as error:
            report['status'] = 'partial_or_unknown'
            report['error'] = str(error)
            raise
        finally:
            if producer is not None:
                try:
                    producer.close(timeout=10)
                except Exception as error:
                    report['producer_close_error'] = str(error)
            output.seek(0)
            json.dump(report, output, indent=2)
            output.truncate()
    print(f"Replayed {len(records)} acknowledged payloads. Manifest: {args.report}")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--trino-url', default=os.getenv('TRINO_URL', 'http://localhost:18080'))
    commands = parser.add_subparsers(dest='command', required=True)
    commands.add_parser('bootstrap')
    query = commands.add_parser('query')
    query.add_argument('sql')
    for name in ('reconcile', 'replay'):
        command = commands.add_parser(name)
        command.add_argument('--from-utc', required=True)
        command.add_argument('--until-utc', required=True)
        command.add_argument('--snapshot', type=int)
        command.add_argument('--execute', action='store_true', help='Commit reconciliation or publish replay; default is preview')
        command.add_argument('--report', type=Path,
            default=Path('.flink-state/lakehouse') / f'{name}-{uuid4()}.json')
        if name == 'reconcile':
            command.add_argument('--window-seconds', type=int, default=300)
        else:
            command.add_argument('--target-topic', required=True)
            command.add_argument('--max-records', type=int, default=10000)
    args = parser.parse_args()
    client = TrinoClient(args.trino_url)
    if args.command == 'bootstrap':
        bootstrap(client)
        print('Revenue reconciliation table is ready.')
    elif args.command == 'query':
        print(json.dumps(client.query(args.sql), indent=2))
    elif args.command == 'reconcile':
        reconcile(client, args)
    elif args.command == 'replay':
        replay(client, args)


if __name__ == '__main__':
    main()
