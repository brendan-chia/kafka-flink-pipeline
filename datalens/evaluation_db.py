"""Database fixtures restricted to databases created by this evaluation harness."""
from contextlib import contextmanager
from copy import deepcopy
import re
import json
from uuid import uuid4
from datetime import datetime, timezone

import psycopg2
from psycopg2 import sql
from psycopg2.extras import Json
from datalens import evidence
from datalens.benchmark import ROOT, START, END, audit_rows
from datalens.investigation import EvidenceService

MARKER = 'datalens-phase-four-v1'


def demo_name(name):
    if not re.fullmatch(r'datalens_demo_[a-z0-9_]{1,40}', name):
        raise ValueError('Demo database must start with datalens_demo_ and use lowercase letters, digits or underscores')
    return name


def initialize(params):
    conn = evidence.connect(params, readonly=False)
    try:
        with conn, conn.cursor() as cur:
            cur.execute((ROOT / 'sql/init.sql').read_text(encoding='utf-8'))
            cur.execute((ROOT / 'sql/migrations/004_datalens_evidence.sql').read_text(encoding='utf-8')
                        .replace('BEGIN;', '').replace('COMMIT;', ''))
            cur.execute('CREATE TABLE public.datalens_evaluation_owner (marker TEXT PRIMARY KEY)')
            cur.execute('INSERT INTO public.datalens_evaluation_owner VALUES (%s)', (MARKER,))
    finally:
        conn.close()


def require_owned(params):
    conn = evidence.connect(params)
    try:
        with conn.cursor() as cur:
            cur.execute('SELECT marker FROM public.datalens_evaluation_owner')
            if cur.fetchall() != [(MARKER,)]:
                raise ValueError('Database ownership marker does not match')
    finally:
        conn.close()


def seed(params, case):
    require_owned(params)
    rows, _ = audit_rows(case)
    arrivals = {}
    for delivery in case['deliveries']:
        event_id = json.loads(delivery['payload']).get('event_id')
        arrivals.setdefault(event_id, datetime.fromtimestamp(delivery['received_timestamp_ms'] / 1000, timezone.utc))
    conn = evidence.connect(params, readonly=False)
    try:
        with conn, conn.cursor() as cur:
            # Only reset the harness-created disposable database, never the configured source DB.
            cur.execute('DELETE FROM datalens.quality_results')
            cur.execute('DELETE FROM public.payment_revenue_windows')
            cur.execute('DELETE FROM public.processed_events')
            for index, row in enumerate(rows):
                cur.execute('''INSERT INTO public.processed_events
                    (event_id,user_id,event_type,event_timestamp_ms,amount,currency,category,
                     ingested_at,processed_at,source_topic,source_partition,source_offset)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,'datalens-benchmark',0,%s)''',
                    (*row[:7], arrivals[row[0]], arrivals[row[0]], index))
            if case['stored'] is not None:
                cur.execute('INSERT INTO public.payment_revenue_windows VALUES (%s,%s,%s,%s,%s)',
                    (START,END,'MYR',case['stored']['count'],case['stored']['revenue']))
            for q in case['quality']:
                cur.execute('INSERT INTO datalens.quality_results VALUES (%s,%s,%s,%s,%s,%s,%s,%s)',
                    (q['result_id'],q['observed_at'],q['check_name'],q['dataset'],q['status'],
                     q['range_start'],q['range_end'],Json(q['evidence'])))
    finally:
        conn.close()


@contextmanager
def benchmark_database():
    admin = evidence.connect(evidence.connection_params(), readonly=False)
    admin.autocommit = True
    name = 'datalens_benchmark_' + uuid4().hex
    created = False
    try:
        with admin.cursor() as cur:
            cur.execute(sql.SQL('CREATE DATABASE {}').format(sql.Identifier(name)))
        created = True
        params = dict(evidence.connection_params(), dbname=name)
        initialize(params)
        def factory(case):
            seed(params, case)
            return TracedService(params)
        yield factory
    finally:
        if created:
            with admin.cursor() as cur:
                cur.execute(sql.SQL('DROP DATABASE {} WITH (FORCE)').format(sql.Identifier(name)))
        admin.close()


class TracedService(EvidenceService):
    def __init__(self, params):
        super().__init__(params)
        self.calls = []

    @contextmanager
    def snapshot(self):
        self.calls.append('snapshot')
        with super().snapshot() as conn:
            yield conn

    def catalogue(self, conn):
        self.calls.append('catalogue')
        return super().catalogue(conn)

    def compare(self, conn, request):
        self.calls.append('compare')
        return super().compare(conn, request)

    def freshness(self, conn):
        self.calls.append('freshness')
        return super().freshness(conn)

    def quality(self, conn, *args, **kwargs):
        self.calls.append('quality')
        return super().quality(conn, *args, **kwargs)


def setup_demo(name, case):
    demo_name(name)
    admin = evidence.connect(evidence.connection_params(), readonly=False)
    admin.autocommit = True
    try:
        with admin.cursor() as cur:
            # CREATE refuses existing databases; no reuse or replacement of user data.
            cur.execute(sql.SQL('CREATE DATABASE {}').format(sql.Identifier(name)))
    finally:
        admin.close()
    params = dict(evidence.connection_params(), dbname=name)
    initialize(params)
    baseline = deepcopy(case)
    baseline['deliveries'] = baseline['deliveries'][:1]
    seed(params, baseline)
    return params


def inject_late(params, case):
    require_owned(params)
    row = audit_rows(case)[0][1]
    conn = evidence.connect(params, readonly=False)
    try:
        with conn, conn.cursor() as cur:
            cur.execute('''INSERT INTO public.processed_events
                (event_id,user_id,event_type,event_timestamp_ms,amount,currency,category,
                 ingested_at,processed_at,source_topic,source_partition,source_offset)
                VALUES (%s,%s,%s,%s,%s,%s,%s,'2026-01-01 00:07','2026-01-01 00:07',
                        'datalens-benchmark',0,1) ON CONFLICT (event_id) DO NOTHING''', row[:7])
    finally:
        conn.close()


def repair_demo(params):
    require_owned(params)
    conn = evidence.connect(params, readonly=False)
    try:
        with conn, conn.cursor() as cur:
            cur.execute('''INSERT INTO public.payment_revenue_windows
                SELECT %s::timestamp,%s::timestamp,'MYR',count(*),sum(amount)
                FROM public.processed_events WHERE event_type='payment' AND currency='MYR'
                  AND event_timestamp_ms >= 1767225600000 AND event_timestamp_ms < 1767225900000
                HAVING count(*) > 0
                ON CONFLICT (window_start,window_end,currency) DO UPDATE
                SET payment_count=EXCLUDED.payment_count,revenue=EXCLUDED.revenue''', (START,END))
    finally:
        conn.close()
