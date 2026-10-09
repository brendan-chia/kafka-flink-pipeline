"""Real PostgreSQL metric SQL and migration test, with all writes rolled back."""
import math
import argparse
import subprocess
from pathlib import Path
import sys
from unittest.mock import patch, Mock
from uuid import uuid4

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from monitoring import metrics
from scripts.benchmark_pipeline import pg_connect


def docker_check():
    """Exercise the exact collector queries when PostgreSQL has no host port."""
    queries = []
    connection = Mock()
    cursor = Mock()
    connection.cursor.return_value.__enter__ = Mock(return_value=cursor)
    connection.cursor.return_value.__exit__ = Mock(return_value=False)
    cursor.execute.side_effect = lambda query: queries.append(query)
    cursor.fetchone.side_effect = [(0, None), (0, None, None)]
    with patch.object(metrics.psycopg2, 'connect', return_value=connection):
        metrics.collect_postgres({})
    root = Path(__file__).resolve().parents[1]
    schema = 'operations_test_' + uuid4().hex
    migration = (root / 'sql/migrations/003_operational_metrics.sql').read_text().replace('BEGIN;', '').replace('COMMIT;', '')
    sql = f"BEGIN; CREATE SCHEMA {schema}; SET search_path TO {schema}; SET TIME ZONE 'UTC';\n"
    sql += (root / 'sql/init.sql').read_text() + migration + migration
    sql += f'CREATE TEMP TABLE empty_metrics (row_count, age) AS {queries[0]};'
    sql += "DO $$ BEGIN IF (SELECT row_count <> 0 OR age IS NOT NULL FROM empty_metrics) THEN RAISE EXCEPTION 'Empty metrics wrong'; END IF; END $$;"
    sql += '''INSERT INTO processed_events
        (event_id,user_id,event_type,amount,currency,event_timestamp_ms,ingested_at,
         processed_at,source_topic,source_partition,source_offset)
        VALUES ('fixture','test','payment',1,'MYR',
        (EXTRACT(EPOCH FROM CURRENT_TIMESTAMP)*1000)::bigint - 5000,
        CURRENT_TIMESTAMP AT TIME ZONE 'UTC' - INTERVAL '2 seconds',
        CURRENT_TIMESTAMP AT TIME ZONE 'UTC','test',0,0);'''
    sql += f'CREATE TEMP TABLE recent_metrics (row_count, event_latency, broker_latency) AS {queries[1]};'
    sql += """DO $$ BEGIN IF (SELECT row_count <> 1 OR abs(event_latency[2]-5) > 0.01
        OR broker_latency[2] <> 2 FROM recent_metrics) THEN RAISE EXCEPTION 'Latency metrics wrong';
        END IF; END $$; ROLLBACK;"""
    subprocess.run(['docker', 'exec', '-i', 'postgres', 'psql', '-v', 'ON_ERROR_STOP=1',
                    '-U', 'grabuser', '-d', 'grabevents'], input=sql, text=True, check=True)
    print('PASS: exact collector SQL and repeatable migration; all writes rolled back.')


def main():
    root = Path(__file__).resolve().parents[1]
    conn = pg_connect()
    schema = 'operations_test_' + uuid4().hex
    try:
        with conn.cursor() as cur:
            cur.execute(f'CREATE SCHEMA {schema}')
            cur.execute(f'SET search_path TO {schema}')
            cur.execute("SET TIME ZONE 'UTC'")
            cur.execute((root / 'sql/init.sql').read_text())
            # Strip transaction statements: the entire test must roll back.
            migration = (root / 'sql/migrations/003_operational_metrics.sql').read_text()
            migration = migration.replace('BEGIN;', '').replace('COMMIT;', '')
            cur.execute(migration)
            cur.execute(migration)
        wrapper = Mock(wraps=conn)
        wrapper.close = Mock()
        with patch.object(metrics.psycopg2, 'connect', return_value=wrapper):
            assert metrics.collect_postgres({}) == 0
            assert math.isnan(metrics.freshness._value.get())
            with conn.cursor() as cur:
                cur.execute('''INSERT INTO processed_events
                    (event_id,user_id,event_type,amount,currency,event_timestamp_ms,
                     ingested_at,processed_at,source_topic,source_partition,source_offset)
                    VALUES ('fixture','test','payment',1,'MYR',
                    (EXTRACT(EPOCH FROM CURRENT_TIMESTAMP)*1000)::bigint - 5000,
                    CURRENT_TIMESTAMP AT TIME ZONE 'UTC' - INTERVAL '2 seconds',
                    CURRENT_TIMESTAMP AT TIME ZONE 'UTC', 'test',0,0)''')
            assert metrics.collect_postgres({}) == 1
            assert metrics.recent_rows._value.get() == 1
            assert abs(metrics.latency.labels(clock='event', quantile='0.95')._value.get() - 5) < 0.01
            assert metrics.latency.labels(clock='broker', quantile='0.95')._value.get() == 2
        print('PASS: empty/recent metric populations, UTC latency and repeatable migration; writes rolled back.')
    finally:
        conn.rollback()
        conn.close()


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--docker', action='store_true', help='Use docker exec when no database host port is published')
    args = parser.parse_args()
    docker_check() if args.docker else main()
