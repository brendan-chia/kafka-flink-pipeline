"""Offline transport, transaction guards, API identity and consent validation."""
from copy import deepcopy
from datetime import datetime, timezone
import json
import unittest
from unittest.mock import MagicMock

from fastapi.testclient import TestClient
import psycopg2

from journeypulse.auth import GatewaySettings, Source, Tokens
from journeypulse.contracts import ROOT
from journeypulse.customer_api import create_app
from journeypulse.profiles import ProfileStore, decode_transport
from journeypulse.processor import configuration, source_ddl

SETTINGS = GatewaySettings((Source('demo-app', 'server-only-secret-123', ('http://localhost:5173',)),),
                           'session-signing-secret-12345678901234567890')


def envelope(index=0):
    event = json.loads((ROOT / 'contracts/journeypulse/examples/valid-journey.json').read_text())[index]
    return dict(transport_version=1, source_id='demo-app', request_id='request-1',
                received_at='2026-10-12T09:00:00Z', event=event)


class ProcessorTests(unittest.TestCase):
    def test_occurrence_is_taken_from_event_not_gateway(self):
        wrapper = envelope()
        parsed, occurred, received = decode_transport(json.dumps(wrapper), 'u123')
        self.assertEqual(parsed, wrapper)
        self.assertNotEqual(occurred, received)
        for payload, key in [(wrapper['event'], 'u123'), (wrapper, 'attacker'),
                             (dict(wrapper, transport_version=True), 'u123'),
                             (dict(wrapper, received_at='invalid'), 'u123')]:
            with self.subTest(payload=payload), self.assertRaises(ValueError):
                decode_transport(json.dumps(payload), key)

    def store(self, rows):
        connection = MagicMock()
        cursor = connection.cursor.return_value.__enter__.return_value
        cursor.fetchone.side_effect = rows
        return ProfileStore(connect=lambda: connection), connection, cursor

    def test_checkpoint_replay_never_reaches_counters(self):
        import hashlib
        payload = json.dumps(envelope())
        store, connection, cursor = self.store([None, (hashlib.sha256(b'\x01'+payload.encode()).hexdigest(),)])
        self.assertEqual(store.apply('journeypulse-events', 0, 1, 'u123', payload), 'replay')
        self.assertFalse(any('UPDATE journeypulse.behavioral_summaries' in c.args[0] for c in cursor.execute.call_args_list))
        connection.close.assert_called_once()

    def test_new_offset_duplicate_or_conflict_never_reaches_counters(self):
        from journeypulse.profiles import fingerprint
        wrapper = envelope()
        matching = fingerprint(dict(source_id=wrapper['source_id'], event=wrapper['event']))
        for digest, expected in [(matching, 'duplicate'), ('changed', 'conflict')]:
            store, connection, cursor = self.store([(1,), None, (digest,)])
            self.assertEqual(store.apply('journeypulse-events', 0, 1, 'u123', json.dumps(wrapper)), expected)
            self.assertFalse(any('UPDATE journeypulse.behavioral_summaries' in c.args[0] for c in cursor.execute.call_args_list))

    def test_late_activity_and_order_identity_are_database_guarded(self):
        store, connection, cursor = self.store([(1,), ('event',), None])
        self.assertEqual(store.apply('journeypulse-events', 0, 1, 'u123', json.dumps(envelope(4))), 'applied')
        calls = cursor.execute.call_args_list
        summary = next(c for c in calls if 'UPDATE journeypulse.behavioral_summaries' in c.args[0])
        self.assertIn('GREATEST(latest_activity_at', summary.args[0])
        self.assertEqual(summary.args[1][2], 0)  # Existing order adds a notification, zero orders.
        self.assertFalse(any('UPDATE journeypulse.customers' in c.args[0] for c in calls))
        self.assertTrue(any('completed_orders' in c.args[0] and 'ON CONFLICT DO NOTHING' in c.args[0] for c in calls))

    def test_failed_summary_raises_and_rolls_back_receipt(self):
        store, connection, cursor = self.store([(1,), ('event',)])
        def fail(sql, *args):
            if 'UPDATE journeypulse.behavioral_summaries' in sql:
                raise psycopg2.OperationalError('unavailable')
        cursor.execute.side_effect = fail
        with self.assertRaises(psycopg2.OperationalError):
            store.apply('journeypulse-events', 0, 1, 'u123', json.dumps(envelope()))
        self.assertIs(connection.__exit__.call_args.args[0], psycopg2.OperationalError)

    def test_invalid_record_is_receipted_without_behavior(self):
        for payload in (b'\xff', None, b'{broken'):
            with self.subTest(payload=payload):
                store, _, cursor = self.store([(1,)])
                self.assertEqual(store.apply('journeypulse-events', 0, 1, 'u123', payload), 'invalid')
                self.assertEqual(len(cursor.execute.call_args_list), 3)

    def test_independent_source_and_checkpoint_configuration(self):
        ddl = source_ddl('kafka:9092', 'journeypulse-profiles-v1')
        self.assertIn("'topic'='journeypulse-events'", ddl)
        self.assertIn("METADATA FROM 'offset'", ddl)
        self.assertIn("'key.format'='raw'", ddl)
        self.assertIn('journeypulse', configuration()['execution.checkpointing.dir'])


class CustomerAPITests(unittest.TestCase):
    def setUp(self):
        self.store = MagicMock()
        now = datetime.now(timezone.utc)
        self.row = dict(user_id='u123', preferred_language='en', marketing_consent=False,
                        consent_updated_at=None, onboarded_at=None, created_at=now,
                        attributes_updated_at=None, event_counts={}, completed_orders=0,
                        latest_activity_at=None, latest_order_at=None, latest_received_at=None,
                        processed_at=None, revision=0, served_at=now)
        self.store.get.side_effect = lambda _: deepcopy(self.row)
        self.store.update.return_value = True
        self.client = TestClient(create_app(SETTINGS, self.store))
        self.addCleanup(self.client.close)
        self.token = Tokens(SETTINGS, audience='journeypulse-customers').issue('demo-app', 'u123', 's456')[0]
        self.headers = {'Authorization': 'Bearer ' + self.token}

    def test_reads_and_updates_require_bound_customer_audience(self):
        for headers in ({}, {'Authorization': 'Bearer broken'}, {'Authorization': 'Bearer ' +
                Tokens(SETTINGS).issue('demo-app', 'u123', 's456')[0]}):
            self.assertEqual(self.client.get('/v1/customers/u123', headers=headers).status_code, 401)
        self.assertEqual(self.client.get('/v1/customers/other', headers=self.headers).status_code, 403)
        self.assertEqual(self.client.patch('/v1/customers/other', headers=self.headers,
                                          json={'marketing_consent': True}).status_code, 403)
        self.store.update.assert_not_called()

    def test_onboarding_uses_token_identity_and_missing_consent_is_ineligible(self):
        response = self.client.post('/v1/customers', headers=self.headers, json={'preferred_language': 'ms'})
        self.assertEqual(response.status_code, 200)
        self.assertFalse(response.json()['marketing_eligible'])
        self.assertEqual(response.json()['freshness']['status'], 'never_processed')
        self.store.update.assert_called_with('u123', {'preferred_language': 'ms'}, onboarding=True)
        self.assertEqual(self.client.post('/v1/customers', headers=self.headers,
            json={'preferred_language': 'en', 'user_id': 'attacker'}).status_code, 422)

    def test_consent_must_be_explicit_boolean_and_patch_preserves_omitted_fields(self):
        for invalid in ('true', 1, None):
            self.assertEqual(self.client.patch('/v1/customers/u123', headers=self.headers,
                json={'marketing_consent': invalid}).status_code, 422)
        response = self.client.patch('/v1/customers/u123', headers=self.headers, json={'preferred_language': 'en-GB'})
        self.assertEqual(response.status_code, 200)
        self.store.update.assert_called_with('u123', {'preferred_language': 'en-GB'})

    def test_missing_database_and_origin_outcomes(self):
        self.store.get.side_effect = None
        self.store.get.return_value = None
        self.assertEqual(self.client.get('/v1/customers/u123', headers=self.headers).status_code, 404)
        self.store.get.side_effect = psycopg2.OperationalError('secret connection details')
        response = self.client.get('/v1/customers/u123', headers=self.headers)
        self.assertEqual(response.status_code, 503)
        self.assertNotIn('secret', response.text)
        self.assertEqual(self.client.get('/v1/customers/u123', headers={**self.headers,
            'Origin': 'https://other.example'}).status_code, 403)

    def test_customer_tokens_are_server_only_and_cannot_track(self):
        body = dict(source_id='demo-app', user_id='u123', session_id='s456')
        headers = {'X-Source-Key': SETTINGS.sources[0].server_key}
        response = self.client.post('/v1/sessions', json=body, headers=headers)
        self.assertEqual(response.status_code, 200)
        with self.assertRaises(ValueError):
            Tokens(SETTINGS).verify(response.json()['access_token'])
        self.assertEqual(self.client.post('/v1/sessions', json=body, headers={**headers,
            'Origin': 'http://localhost:5173'}).status_code, 403)


if __name__ == '__main__':
    unittest.main()
