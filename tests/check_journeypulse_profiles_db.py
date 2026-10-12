"""Executable real PostgreSQL sink correctness tests (requires bootstrap)."""
from concurrent.futures import ThreadPoolExecutor
from copy import deepcopy
from datetime import datetime, timedelta, timezone
import json
import os
import sys
from pathlib import Path
import unittest
from uuid import uuid4

import psycopg2
from psycopg2.extensions import cursor as PgCursor
from fastapi.testclient import TestClient

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from journeypulse.profiles import ProfileStore
from journeypulse.auth import GatewaySettings, Tokens
from journeypulse.customer_api import create_app


class DurableProfileTests(unittest.TestCase):
    def setUp(self):
        self.store = ProfileStore()
        self.user = 'dbcheck-' + uuid4().hex
        self.offset = uuid4().int % (2**62)
        self.now = datetime.now(timezone.utc).replace(microsecond=0)
        self.payload = dict(transport_version=1, request_id=uuid4().hex, source_id='demo-app',
            received_at=self.now.isoformat(), event=dict(event_id=uuid4().hex,
            event_name='session_started', schema_version=1, user_id=self.user,
            session_id='dbcheck', occurred_at=self.now.isoformat(), properties={'platform': 'web'}))

    def apply(self, payload=None, offset=None, store=None):
        # Synthetic partition 99 isolates direct sink checks from real Kafka partitions.
        return (store or self.store).apply('journeypulse-events', 99,
            self.offset if offset is None else offset, self.user, json.dumps(payload or self.payload))

    def next(self, **changes):
        payload = deepcopy(self.payload)
        payload['event'].update(event_id=uuid4().hex, **changes)
        return payload

    def test_duplicate_delivery_and_new_processor_replay(self):
        self.assertEqual(self.apply(), 'applied')
        self.assertEqual(self.apply(offset=self.offset+1), 'duplicate')
        before = self.store.get(self.user)
        self.assertEqual(self.apply(store=ProfileStore()), 'replay')
        after = self.store.get(self.user)
        for field in ('event_counts', 'revision', 'processed_at'):
            self.assertEqual(before[field], after[field])
        self.assertEqual(after['event_counts'], {'session_started': 1})

    def test_late_event_and_old_snapshot_replay_do_not_regress(self):
        self.apply()
        late = self.next(occurred_at=(self.now-timedelta(days=2)).isoformat())
        self.apply(late, self.offset+1)
        self.assertEqual(self.store.get(self.user)['latest_activity_at'], self.now)
        self.apply(store=ProfileStore())
        self.assertEqual(self.store.get(self.user)['event_counts']['session_started'], 2)

    def test_distinct_orders_not_notifications(self):
        props = dict(order_id='order-1', cart_id='cart-1', restaurant_id='restaurant-1')
        order = self.next(event_name='order_completed', properties=props)
        self.apply(order)
        repeated = deepcopy(order)
        repeated['event']['event_id'] = uuid4().hex
        repeated['event']['occurred_at'] = (self.now+timedelta(seconds=1)).isoformat()
        self.apply(repeated, self.offset+1)
        row = self.store.get(self.user)
        self.assertEqual(row['event_counts']['order_completed'], 2)
        self.assertEqual(row['completed_orders'], 1)
        self.assertEqual(row['latest_order_at'], self.now)

    def test_events_before_onboarding_and_consent_survive(self):
        self.apply()
        row = self.store.get(self.user)
        self.assertIsNone(row['onboarded_at'])
        self.assertFalse(row['marketing_consent'])
        self.store.update(self.user, dict(preferred_language='ms', marketing_consent=True), onboarding=True)
        self.apply(self.next(), self.offset+1)
        row = self.store.get(self.user)
        self.assertTrue(row['marketing_consent'])
        self.assertEqual(row['preferred_language'], 'ms')
        self.store.update(self.user, dict(preferred_language='en'), onboarding=True)
        self.assertTrue(self.store.get(self.user)['marketing_consent'])
        self.store.update(self.user, dict(marketing_consent=False))
        self.apply(self.next(), self.offset+2)
        self.assertFalse(self.store.get(self.user)['marketing_consent'])

    def test_onboarding_before_events_defaults_consent_false(self):
        self.store.update(self.user, dict(preferred_language='en'), onboarding=True)
        self.apply()
        row = self.store.get(self.user)
        self.assertFalse(row['marketing_consent'])
        self.assertIsNotNone(row['onboarded_at'])

    def test_conflicting_event_id_first_payload_wins(self):
        self.apply()
        changed = deepcopy(self.payload)
        changed['event']['properties']['platform'] = 'ios'
        self.assertEqual(self.apply(changed, self.offset+1), 'conflict')
        self.assertEqual(self.store.get(self.user)['revision'], 1)

    def test_expired_id_new_delivery_counts_but_old_offset_never_does(self):
        self.apply()
        connection = self.store.connect()
        try:
            with connection, connection.cursor() as cursor:
                cursor.execute("UPDATE journeypulse.event_ids SET expires_at=clock_timestamp()-interval '1 second' WHERE event_id=%s",
                               (self.payload['event']['event_id'],))
        finally:
            connection.close()
        self.store.prune_event_ids()
        self.assertEqual(self.apply(store=ProfileStore()), 'replay')
        self.assertEqual(self.apply(offset=self.offset+1), 'applied')
        self.assertEqual(self.store.get(self.user)['event_counts']['session_started'], 2)

    def test_concurrent_deliveries_and_distinct_events(self):
        with ThreadPoolExecutor(max_workers=4) as pool:
            outcomes = list(pool.map(lambda i: self.apply(offset=self.offset+i), range(8)))
        self.assertEqual(outcomes.count('applied'), 1)
        self.assertEqual(outcomes.count('duplicate'), 7)
        payloads = [self.next() for _ in range(8)]
        with ThreadPoolExecutor(max_workers=4) as pool:
            list(pool.map(lambda i: self.apply(payloads[i], self.offset+8+i), range(8)))
        self.assertEqual(self.store.get(self.user)['event_counts']['session_started'], 9)

    def test_failure_after_identity_inserts_rolls_back_then_retries(self):
        class FailingCursor(PgCursor):
            def execute(self, query, parameters=None):
                if 'UPDATE journeypulse.behavioral_summaries' in query:
                    raise RuntimeError('injected failure before summary write')
                return super().execute(query, parameters)
        failing = ProfileStore(connect=lambda: psycopg2.connect(
            **self.store.settings.database_parameters(), cursor_factory=FailingCursor))
        with self.assertRaises(RuntimeError):
            self.apply(store=failing)
        self.assertIsNone(self.store.get(self.user))
        self.assertEqual(self.apply(), 'applied')
        self.assertEqual(self.store.get(self.user)['revision'], 1)

    def test_invalid_records_and_tombstones_do_not_create_profiles(self):
        self.assertEqual(self.store.apply('journeypulse-events', 99, self.offset,
                                        self.user, None), 'invalid')
        self.assertEqual(self.store.apply('journeypulse-events', 99, self.offset,
                                        self.user, None), 'replay')
        self.assertEqual(self.store.apply('journeypulse-events', 99, self.offset+1,
                                        'wrong-user', json.dumps(self.payload)), 'invalid')
        self.assertIsNone(self.store.get(self.user))

    def test_real_database_api_onboarding_and_behavior_preservation(self):
        settings = GatewaySettings.from_environment()
        source = settings.sources[0].source_id
        token = Tokens(settings, audience='journeypulse-customers').issue(source, self.user, 'dbcheck')[0]
        headers = {'Authorization': 'Bearer '+token}
        with TestClient(create_app(settings, self.store)) as client:
            self.assertEqual(client.get('/v1/customers/'+self.user, headers=headers).status_code, 404)
            self.apply()
            self.assertFalse(client.get('/v1/customers/'+self.user, headers=headers).json()['marketing_eligible'])
            response = client.post('/v1/customers', headers=headers,
                                   json={'preferred_language': 'ms', 'marketing_consent': True})
            self.assertEqual(response.status_code, 200, response.text)
            self.assertTrue(response.json()['marketing_eligible'])
            self.apply(self.next(), self.offset+1)
            response = client.get('/v1/customers/'+self.user, headers=headers)
            self.assertEqual(response.json()['event_counts']['session_started'], 2)
            self.assertTrue(response.json()['marketing_consent'])
            self.assertIsNotNone(response.json()['freshness']['processed_at'])
            self.assertEqual(client.get('/v1/customers/other', headers=headers).status_code, 403)
            response = client.patch('/v1/customers/'+self.user, headers=headers,
                                    json={'marketing_consent': False})
            self.assertFalse(response.json()['marketing_eligible'])

    @unittest.skipUnless(os.getenv('JOURNEYPULSE_CHECK_ROLES') == '1', 'optional roles.sql not requested')
    def test_processor_and_serving_database_privileges(self):
        def connect_as(role):
            connection = self.store.connect()
            with connection.cursor() as cursor:
                cursor.execute('SET ROLE '+role)  # Fixed, test-owned role names only.
            return connection
        processor = ProfileStore(connect=lambda: connect_as('journeypulse_processor'))
        serving = ProfileStore(connect=lambda: connect_as('journeypulse_serving'))
        self.assertEqual(self.apply(store=processor), 'applied')
        with self.assertRaises(psycopg2.errors.InsufficientPrivilege):
            processor.update(self.user, {'marketing_consent': True})
        self.assertTrue(serving.update(self.user, dict(preferred_language='ms', marketing_consent=True), onboarding=True))
        self.assertEqual(serving.get(self.user)['preferred_language'], 'ms')
        with self.assertRaises(psycopg2.errors.InsufficientPrivilege):
            self.apply(self.next(), self.offset+1, store=serving)


if __name__ == '__main__':
    unittest.main(verbosity=2)
