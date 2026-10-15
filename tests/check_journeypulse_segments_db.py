"""Real PostgreSQL segment/receipt/recovery checks; only mutate unique test users.

ClockCursor controls evaluator time without sleeping or changing production clocks.
Requires bootstrap and an owner test connection, as the Phase 3 DB probe does.
"""
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
import json
import os
from pathlib import Path
import sys
import unittest
from uuid import uuid4

import psycopg2
from psycopg2.extensions import cursor as PgCursor
from fastapi.testclient import TestClient

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from journeypulse.auth import GatewaySettings, Tokens
from journeypulse.customer_api import create_app
from journeypulse.profiles import ProfileStore
from journeypulse.segments import SegmentSettings, register_rules

SHORT = SegmentSettings(window_seconds=10, abandon_seconds=10, poll_seconds=1, stale_seconds=30)


class SegmentDatabaseTests(unittest.TestCase):
    def setUp(self):
        self.user = 'segmentdb-' + uuid4().hex
        self.offset = uuid4().int % (2**62)
        self.start = self.now = datetime.now(timezone.utc)
        parameters = ProfileStore().settings.database_parameters()
        case = self

        class ClockCursor(PgCursor):
            def execute(self, query, parameters=None):
                if query == 'SELECT clock_timestamp()':
                    return super().execute('SELECT %s::timestamptz', (case.now,))
                return super().execute(query, parameters)

        self.connect = lambda: psycopg2.connect(**parameters, cursor_factory=ClockCursor)
        self.store = ProfileStore(connect=self.connect, segment_settings=SHORT)
        with self.connect() as connection, connection.cursor() as cursor:
            register_rules(cursor, SHORT, create=True)
        self.store.update(self.user, dict(preferred_language='en', marketing_consent=True), onboarding=True)

    def event(self, name, cart='a', order='o1', when=None):
        props = dict(restaurant_id='r1')
        if name == 'add_to_cart':
            props.update(cart_id=cart, item_id='i1', quantity=1)
        elif name == 'checkout_started':
            props = dict(cart_id=cart)
        elif name == 'order_completed':
            props.update(cart_id=cart, order_id=order)
        return dict(transport_version=1, request_id=uuid4().hex, source_id='demo-app',
            received_at=self.start.isoformat(), event=dict(schema_version=1,event_id=uuid4().hex,
            user_id=self.user, session_id='segmentdb', event_name=name,
            occurred_at=(when or self.start).isoformat(), properties=props))

    def apply(self, event, store=None, offset=None):
        self.offset += 1
        return (store or self.store).apply('journeypulse-events', 98,
            self.offset if offset is None else offset, self.user, json.dumps(event))

    def tick(self, seconds, store=None):
        self.now = self.start + timedelta(seconds=seconds)
        # Force only this test user's durable schedule due; evaluation time is injected.
        with self.connect() as connection, connection.cursor() as cursor:
            cursor.execute('''UPDATE journeypulse.segment_evaluations
                SET next_evaluation_at=clock_timestamp()-interval '1 second' WHERE user_id=%s''', (self.user,))
        return (store or self.store).evaluate_due(self.user)

    def member(self, name):
        return self.store.targeting(self.user)['segments'][name]

    def test_threshold_duplicate_delivery_silent_expiry_and_restart(self):
        for _ in range(4):
            self.apply(self.event('restaurant_view'))
        self.assertFalse(self.member('active_browser')['member'])
        fifth = self.event('restaurant_view')
        self.assertEqual(self.apply(fifth), 'applied')
        offset = self.offset
        self.assertTrue(self.member('active_browser')['eligible'])
        self.assertEqual(self.apply(fifth), 'duplicate')
        recovered = ProfileStore(connect=self.connect, segment_settings=SHORT)
        self.assertEqual(self.apply(fifth, recovered, offset), 'replay')
        self.assertEqual(self.store.get(self.user)['event_counts']['restaurant_view'], 5)
        self.assertEqual(self.tick(10, recovered), 1)
        self.assertFalse(self.member('active_browser')['member'])
        self.assertEqual(self.member('active_browser')['evidence']['count'], 0)

    def test_separate_carts_checkout_delayed_completion_terminal_replay(self):
        add = self.event('add_to_cart')
        self.apply(add)
        original_offset = self.offset
        self.apply(self.event('add_to_cart', cart='b'))
        self.apply(self.event('checkout_started', when=self.start+timedelta(seconds=8)))
        self.tick(10)
        self.assertEqual(self.member('abandoned_cart')['evidence']['count'], 2)
        self.apply(self.event('order_completed', when=self.start+timedelta(seconds=2)))
        self.assertEqual(self.member('abandoned_cart')['evidence']['count'], 1)
        self.assertEqual(self.apply(add), 'duplicate')
        self.assertEqual(self.apply(add, offset=original_offset), 'replay')
        # After the Phase 3 event-ID horizon, a new offset may apply, but cannot reopen A.
        with self.connect() as connection, connection.cursor() as cursor:
            cursor.execute('UPDATE journeypulse.event_ids SET expires_at=clock_timestamp() WHERE event_id=%s',
                           (add['event']['event_id'],))
        self.assertEqual(self.apply(add), 'applied')
        self.apply(self.event('add_to_cart', when=self.start+timedelta(seconds=20)))
        self.tick(40, ProfileStore(connect=self.connect, segment_settings=SHORT))
        self.assertEqual(self.member('abandoned_cart')['evidence']['count'], 1)
        self.apply(self.event('order_completed', cart='b', order='o2', when=self.start+timedelta(seconds=1)))
        self.assertFalse(self.member('abandoned_cart')['eligible'])

    def test_latest_add_postpones_but_old_delayed_add_does_not(self):
        self.apply(self.event('add_to_cart'))
        self.apply(self.event('add_to_cart', when=self.start+timedelta(seconds=5)))
        self.apply(self.event('add_to_cart', when=self.start-timedelta(days=1)))
        self.tick(10)
        self.assertFalse(self.member('abandoned_cart')['member'])
        self.tick(15)
        self.assertTrue(self.member('abandoned_cart')['eligible'])

    def test_completion_before_add_and_reused_order_facts(self):
        self.apply(self.event('order_completed'))
        self.apply(self.event('add_to_cart'))
        self.apply(self.event('add_to_cart', cart='b'))
        # Existing order identity cannot be reassigned to B by another notification.
        self.apply(self.event('order_completed', cart='b'))
        self.tick(20)
        self.assertEqual(self.member('abandoned_cart')['evidence']['count'], 1)
        self.assertEqual(self.store.get(self.user)['completed_orders'], 1)

    def test_distinct_orders_consent_revocation_and_api_snapshot(self):
        for index in range(3):
            order = self.event('order_completed', cart=str(index), order=str(index))
            self.apply(order)
            self.assertEqual(self.apply(order), 'duplicate')
            self.apply(self.event('order_completed', cart=str(index), order=str(index)))
            self.assertEqual(self.member('repeat_customer')['member'], index == 2)
        settings = GatewaySettings.from_environment()
        token = Tokens(settings, audience='journeypulse-customers').issue(settings.sources[0].source_id,
                                                                         self.user, 'segmentdb')[0]
        headers = {'Authorization': 'Bearer '+token}
        with TestClient(create_app(settings, self.store)) as client:
            path = '/v1/customers/'+self.user+'/targeting'
            self.assertTrue(client.get(path, headers=headers).json()['eligible'])
            response = client.patch('/v1/customers/'+self.user, headers=headers, json={'marketing_consent': False})
            self.assertEqual(response.status_code, 200)
            self.assertFalse(client.get(path, headers=headers).json()['eligible'])
            self.assertTrue(self.member('repeat_customer')['member'])
            self.store.update(self.user, {'marketing_consent': True})
            self.assertTrue(client.get(path, headers=headers).json()['eligible'])
        self.assertEqual(self.store.get(self.user)['completed_orders'], 3)

    def test_downtime_and_stale_or_unevaluated_targeting(self):
        self.assertEqual(self.store.targeting(self.user)['freshness']['status'], 'unevaluated')
        self.apply(self.event('add_to_cart'))
        with self.connect() as connection, connection.cursor() as cursor:
            cursor.execute('''UPDATE journeypulse.segment_evaluations
                SET evaluated_at=clock_timestamp()-interval '31 seconds' WHERE user_id=%s''', (self.user,))
        self.assertEqual(self.store.targeting(self.user)['freshness']['status'], 'stale')
        self.assertFalse(self.store.targeting(self.user)['eligible'])
        recovered = ProfileStore(connect=self.connect, segment_settings=SHORT)
        self.tick(100, recovered)
        self.assertTrue(self.member('abandoned_cart')['eligible'])

    def test_concurrent_add_completion_and_scheduler_preserve_terminal_state(self):
        self.apply(self.event('add_to_cart'))
        self.tick(20)
        complete = self.event('order_completed')
        added = self.event('add_to_cart', when=self.start+timedelta(seconds=5))
        self.offset += 10
        with ThreadPoolExecutor(max_workers=3) as pool:
            tasks = [pool.submit(self.store.apply, 'journeypulse-events', 98, self.offset+i,
                                 self.user, json.dumps(event)) for i, event in enumerate((complete, added))]
            tasks.append(pool.submit(self.store.evaluate_due, self.user))
            for task in tasks:
                task.result()
        self.assertFalse(self.member('abandoned_cart')['member'])

    def test_failure_after_membership_write_rolls_back_all_effects(self):
        parameters = self.store.settings.database_parameters()
        class FailingCursor(PgCursor):
            def execute(self, query, parameters=None):
                if 'INSERT INTO journeypulse.segment_evaluations' in query:
                    raise RuntimeError('injected failure after memberships')
                return super().execute(query, parameters)
        failing = ProfileStore(segment_settings=SHORT, connect=lambda: psycopg2.connect(
            **parameters, cursor_factory=FailingCursor))
        event = self.event('add_to_cart')
        coordinate = self.offset+1
        with self.assertRaises(RuntimeError):
            self.apply(event, failing, coordinate)
        self.assertEqual(self.store.targeting(self.user)['freshness']['status'], 'unevaluated')
        self.assertEqual(self.apply(event, offset=coordinate), 'applied')
        self.assertEqual(self.store.get(self.user)['revision'], 1)

    def test_phase3_upgrade_backfills_completed_carts_and_repeat_customer(self):
        with self.connect() as connection, connection.cursor() as cursor:
            cursor.execute('''INSERT INTO journeypulse.behavioral_summaries(user_id,completed_orders)
                VALUES (%s,3)''', (self.user,))
            for index in range(3):
                cursor.execute('''INSERT INTO journeypulse.completed_orders
                    (user_id,order_id,cart_id,restaurant_id,occurred_at) VALUES (%s,%s,%s,'r1',%s)''',
                    (self.user, str(index), str(index), self.start))
            migration = Path(__file__).resolve().parents[1] / 'journeypulse/sql/003_segments.sql'
            cursor.execute(migration.read_text(encoding='utf-8'))
            cursor.execute(migration.read_text(encoding='utf-8'))
        self.assertEqual(self.store.evaluate_due(self.user), 1)
        self.assertTrue(self.member('repeat_customer')['eligible'])
        self.apply(self.event('add_to_cart', cart='0'))
        self.tick(20)
        self.assertFalse(self.member('abandoned_cart')['member'])


    @unittest.skipUnless(os.getenv('JOURNEYPULSE_CHECK_ROLES') == '1', 'optional roles.sql not requested')
    def test_segment_capability_roles(self):
        def as_role(role):
            connection = self.connect()
            with connection.cursor() as cursor:
                cursor.execute('SET ROLE '+role)
            return connection
        processor = ProfileStore(connect=lambda: as_role('journeypulse_processor'), segment_settings=SHORT)
        serving = ProfileStore(connect=lambda: as_role('journeypulse_serving'), segment_settings=SHORT)
        self.apply(self.event('add_to_cart'), processor)
        self.tick(20, processor)
        self.assertTrue(serving.targeting(self.user)['eligible'])
        with self.assertRaises(psycopg2.errors.InsufficientPrivilege):
            serving.evaluate_due(self.user)
        with self.assertRaises(psycopg2.errors.InsufficientPrivilege):
            processor.update(self.user, {'marketing_consent': False})


if __name__ == '__main__':
    unittest.main(verbosity=2)
