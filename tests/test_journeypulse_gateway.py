"""HTTP-level ingestion, source binding, validation and failure behavior."""
import asyncio
import base64
from copy import deepcopy
from dataclasses import replace
from datetime import datetime, timezone
import json
import unittest
import httpx
from unittest.mock import MagicMock
from types import SimpleNamespace

from fastapi.testclient import TestClient

from journeypulse.auth import GatewaySettings, Source, Tokens
from journeypulse.config import EVENT_TOPIC, QUARANTINE_TOPIC
from journeypulse.contracts import ROOT
from journeypulse.gateway import create_app
from journeypulse.publisher import KafkaPublisher, Record

NOW = datetime(2026, 10, 12, 8, tzinfo=timezone.utc).timestamp()
SETTINGS = GatewaySettings((Source('demo-app', 'server-only-secret-123', ('http://localhost:5173',)),),
                           'session-signing-secret-12345678901234567890')


class FixturePublisher:
    def __init__(self):
        self.records = []
        self.outcomes = None
        self.available = True
        self.closed = False

    def publish_many(self, records, timeout):
        self.records.extend(records)
        if self.outcomes is not None:
            return self.outcomes
        return [dict(topic=r.topic, partition=1, offset=i) if self.available else None
                for i, r in enumerate(records)]

    def ready(self):
        return self.available

    def close(self):
        self.closed = True


class GatewayTests(unittest.TestCase):
    def setUp(self):
        self.publisher = FixturePublisher()
        self.client = TestClient(create_app(SETTINGS, self.publisher, clock=lambda: NOW))
        self.addCleanup(self.client.close)
        self.events = json.loads((ROOT / 'contracts/journeypulse/examples/valid-journey.json').read_text())
        for event in self.events:
            event['occurred_at'] = '2026-10-12T08:00:00Z'
        self.token = Tokens(SETTINGS, clock=lambda: NOW).issue('demo-app', 'u123', 's456')[0]
        self.headers = {'Authorization': 'Bearer ' + self.token}

    def post(self, events=None, **kwargs):
        return self.client.post('/v1/events', json={'events': self.events if events is None else events},
                                headers=self.headers, **kwargs)

    def test_all_five_events_are_acknowledged_and_keyed_by_verified_user(self):
        response = self.post()
        self.assertEqual(response.status_code, 200, response.text)
        self.assertEqual(response.json()['counts'], dict(accepted=5, rejected=0, retryable=0))
        self.assertEqual([r.key for r in self.publisher.records], ['u123'] * 5)
        for index, record in enumerate(self.publisher.records):
            self.assertEqual(record.topic, EVENT_TOPIC)
            self.assertEqual(record.value['event'], self.events[index])
            self.assertEqual(record.value['source_id'], 'demo-app')
            self.assertEqual(record.value['received_at'], '2026-10-12T08:00:00Z')
            self.assertEqual(response.json()['results'][index]['delivery']['offset'], index)

    def test_mixed_batch_returns_validation_paths_and_preserves_exact_raw_request(self):
        invalid = deepcopy(self.events[2])
        invalid['properties']['quantity'] = 0
        del invalid['properties']['item_id']
        raw = json.dumps({'events': [self.events[0], invalid]}, indent=3).encode()
        response = self.client.post('/v1/events', content=raw,
            headers={**self.headers, 'Content-Type': 'application/json'})
        self.assertEqual(response.status_code, 207)
        rejected = response.json()['results'][1]
        self.assertEqual(rejected['status'], 'rejected')
        self.assertTrue(rejected['quarantine_persisted'])
        self.assertEqual({e['path'] for e in rejected['errors']},
                         {'$.properties', '$.properties.quantity'})
        record = self.publisher.records[1]
        self.assertEqual(record.topic, QUARANTINE_TOPIC)
        self.assertEqual(base64.b64decode(record.value['raw_request_base64']), raw)
        self.assertEqual(record.value['batch_index'], 1)

    def test_partial_ack_does_not_mark_other_events_accepted(self):
        self.publisher.outcomes = [dict(topic=EVENT_TOPIC, partition=0, offset=2), None]
        response = self.post(self.events[:2])
        self.assertEqual(response.status_code, 207)
        self.assertEqual([r['status'] for r in response.json()['results']], ['accepted', 'retryable'])
        self.assertEqual(response.headers['Retry-After'], '1')

    def test_kafka_outage_returns_retryable_and_unhealthy_readiness(self):
        self.publisher.available = False
        response = self.post()
        self.assertEqual(response.status_code, 503)
        self.assertTrue(all(r['status'] == 'retryable' for r in response.json()['results']))
        self.assertEqual(self.client.get('/health/live').status_code, 200)
        self.assertEqual(self.client.get('/v1/health/ready').status_code, 503)

    def test_quarantine_failure_is_visible_without_retrying_permanent_bad_event(self):
        self.publisher.available = False
        invalid = dict(self.events[0], schema_version=9)
        response = self.post([invalid])
        self.assertEqual(response.status_code, 207)
        result = response.json()['results'][0]
        self.assertEqual(result['status'], 'rejected')
        self.assertFalse(result['quarantine_persisted'])

    def test_body_failures_are_quarantined_after_authentication(self):
        cases = [(b'{broken', 400), (b'{"events":[]}', 422),
                 (b'{"events":[],"events":[]}', 400), (b'{"events":[NaN]}', 400),
                 (b'[]', 422), (b'{"events":"bad"}', 422), (b'\xff', 400)]
        for raw, expected in cases:
            with self.subTest(raw=raw):
                response = self.client.post('/v1/events', content=raw,
                    headers={**self.headers, 'Content-Type': 'application/json'})
                self.assertEqual(response.status_code, expected, response.text)
                self.assertTrue(response.json()['detail']['quarantine_persisted'])
                self.assertEqual(base64.b64decode(self.publisher.records[-1].value['raw_request_base64']), raw)

    def test_malformed_body_quarantine_outage_returns_503(self):
        self.publisher.available = False
        response = self.client.post('/v1/events', content=b'{broken',
            headers={**self.headers, 'Content-Type': 'application/json'})
        self.assertEqual(response.status_code, 503)
        self.assertEqual(response.json()['detail']['code'], 'quarantine_unavailable')

    def test_wrong_identity_is_never_accepted_or_used_as_partition_key(self):
        for field in ('user_id', 'session_id'):
            response = self.post([dict(self.events[0], **{field: 'attacker'})])
            self.assertEqual(response.json()['results'][0]['status'], 'rejected')
            self.assertEqual(response.json()['results'][0]['errors'][0]['rule'], 'identity')
            self.assertEqual(self.publisher.records[-1].key, 'u123')

    def test_future_clock_skew_and_delayed_events(self):
        response = self.post([dict(self.events[0], occurred_at='2026-10-12T08:01:01Z')])
        self.assertEqual(response.json()['results'][0]['errors'][0]['rule'], 'future_timestamp')
        response = self.post([dict(self.events[0], occurred_at='2026-01-01T00:00:00Z')])
        self.assertEqual(response.json()['results'][0]['status'], 'accepted')
        response = self.post([dict(self.events[0], occurred_at='2026-10-12T16:00:00+08:00')])
        self.assertEqual(response.status_code, 200)

    def test_authentication_precedes_ingestion(self):
        for header in ({}, {'Authorization': 'Bearer broken'},
                       {'Authorization': 'Basic ' + self.token}):
            response = self.client.post('/v1/events', content=b'{broken',
                headers={'Content-Type': 'application/json', **header})
            self.assertEqual(response.status_code, 401)
        self.assertEqual(self.publisher.records, [])

    def test_oversize_batches_content_types_and_encodings(self):
        response = self.client.post('/v1/events', content=b'x' * (SETTINGS.max_body_bytes + 1),
            headers={**self.headers, 'Content-Type': 'application/json'})
        self.assertEqual(response.status_code, 413)
        self.assertEqual(self.publisher.records, [])
        self.assertEqual(self.client.post('/v1/events', content='{}', headers=self.headers).status_code, 415)
        self.assertEqual(self.client.post('/v1/events', content='{}', headers={
            **self.headers, 'Content-Type': 'application/json', 'Content-Encoding': 'gzip'}).status_code, 415)
        response = self.post([self.events[0]] * 51)
        self.assertEqual(response.status_code, 422)

    def test_bounded_error_messages_fit_quarantine(self):
        event = dict(self.events[0], event_name='x' * 10000)
        result = self.post([event]).json()['results'][0]
        self.assertTrue(all(len(error['message']) <= 256 for error in result['errors']))

    def test_origin_is_bound_to_source_even_with_global_cors_allowlist(self):
        self.headers['Origin'] = 'http://localhost:5173'
        response = self.post(self.events[:1])
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.headers['Access-Control-Allow-Origin'], 'http://localhost:5173')
        self.headers['Origin'] = 'https://other.example'
        self.assertEqual(self.post(self.events[:1]).status_code, 403)
        preflight = self.client.options('/v1/events', headers={
            'Origin': 'http://localhost:5173', 'Access-Control-Request-Method': 'POST',
            'Access-Control-Request-Headers': 'authorization,content-type'})
        self.assertEqual(preflight.status_code, 200)

    def test_source_session_issuance_is_server_only(self):
        body = dict(source_id='demo-app', user_id='u123', session_id='s456')
        response = self.client.post('/v1/sessions', json=body, headers={'X-Source-Key': 'server-only-secret-123'})
        self.assertEqual(response.status_code, 200)
        claims = Tokens(SETTINGS, clock=lambda: NOW).verify(response.json()['access_token'])
        self.assertEqual(claims['user_id'], 'u123')
        for headers, expected in [
            ({}, 401), ({'X-Source-Key': 'wrong'}, 401),
            ({'X-Source-Key': 'server-only-secret-123', 'Origin': 'http://localhost:5173'}, 403)]:
            with self.subTest(headers=headers):
                self.assertEqual(self.client.post('/v1/sessions', json=body, headers=headers).status_code, expected)
        response = self.client.post('/v1/sessions', json=dict(body, admin=True),
            headers={'X-Source-Key': 'server-only-secret-123'})
        self.assertEqual(response.status_code, 422)

    def test_contract_api_requires_token_and_hides_keys(self):
        self.assertEqual(self.client.get('/v1/contracts').status_code, 401)
        response = self.client.get('/v1/contracts', headers=self.headers)
        self.assertEqual(len(response.json()['contracts']), 5)
        self.assertNotIn(SETTINGS.signing_key, response.text)
        self.assertNotIn(SETTINGS.sources[0].server_key, response.text)
        self.assertEqual(response.headers['Cache-Control'], 'no-store')

    def test_rate_limit_has_a_retry_after(self):
        publisher = FixturePublisher()
        client = TestClient(create_app(replace(SETTINGS, requests_per_minute=1),
                                       publisher, clock=lambda: NOW))
        with client:
            self.assertEqual(client.post('/v1/events', json={'events': self.events[:1]},
                                         headers=self.headers).status_code, 200)
            response = client.post('/v1/events', json={'events': self.events[:1]}, headers=self.headers)
            self.assertEqual(response.status_code, 429)
            self.assertEqual(response.headers['Retry-After'], '1')

    def test_metrics_have_bounded_labels_and_sanitized_failure(self):
        self.post(self.events[:1])
        self.publisher.available = False
        self.post(self.events[:1])
        metrics = self.client.get('/metrics').text
        self.assertIn('journeypulse_gateway_events_total{status="accepted"} 1.0', metrics)
        self.assertIn('journeypulse_gateway_kafka_failures_total{channel="events"} 1.0', metrics)
        self.assertNotIn('u123', metrics)
        self.assertNotIn(self.token, metrics)

    def test_streamed_body_limit_applies_without_content_length(self):
        async def run():
            async def chunks():
                yield b'x' * 100
                yield b'x' * 200
            publisher = FixturePublisher()
            app = create_app(replace(SETTINGS, max_body_bytes=256), publisher, clock=lambda: NOW)
            async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url='http://test') as client:
                response = await client.post('/v1/events', content=chunks(),
                    headers={**self.headers, 'Content-Type': 'application/json'})
            self.assertEqual(response.status_code, 413)
            self.assertEqual(publisher.records, [])
        asyncio.run(run())

    def test_slow_request_body_times_out_without_publication(self):
        async def run():
            async def chunks():
                yield b'{'
                await asyncio.sleep(0.1)
                yield b'}'
            publisher = FixturePublisher()
            settings = replace(SETTINGS, request_timeout_seconds=0.02, publish_timeout_seconds=0.01)
            app = create_app(settings, publisher, clock=lambda: NOW)
            async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url='http://test') as client:
                response = await client.post('/v1/events', content=chunks(),
                    headers={**self.headers, 'Content-Type': 'application/json'})
            self.assertEqual(response.status_code, 408)
            self.assertEqual(publisher.records, [])
        asyncio.run(run())

    def test_inflight_requests_are_bounded(self):
        async def run():
            import threading
            started, release = threading.Event(), threading.Event()
            class BlockingPublisher(FixturePublisher):
                def publish_many(inner, records, timeout):
                    started.set()
                    release.wait(timeout=1)
                    return super().publish_many(records, timeout)
            app = create_app(replace(SETTINGS, max_inflight_requests=1), BlockingPublisher(), clock=lambda: NOW)
            async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url='http://test') as client:
                first = asyncio.create_task(client.post('/v1/events', json={'events': self.events[:1]}, headers=self.headers))
                try:
                    self.assertTrue(await asyncio.to_thread(started.wait, 1))
                    second = await client.post('/v1/events', json={'events': self.events[:1]}, headers=self.headers)
                    self.assertEqual(second.status_code, 429)
                    self.assertEqual(second.json()['detail']['code'], 'gateway_busy')
                finally:
                    release.set()
                    await first
        asyncio.run(run())

    def test_lifespan_closes_publisher(self):
        publisher = FixturePublisher()
        with TestClient(create_app(SETTINGS, publisher)):
            pass
        self.assertTrue(publisher.closed)


class TokenTests(unittest.TestCase):
    def test_expiry_signature_and_signing_key_rotation(self):
        signer = Tokens(SETTINGS, clock=lambda: NOW)
        token = signer.issue('demo-app', 'u123', 's456')[0]
        for invalid in ('broken', token + 'x', token.split('.')[0] + '.wrong'):
            with self.subTest(token=invalid), self.assertRaises(ValueError):
                signer.verify(invalid)
        with self.assertRaises(ValueError):
            Tokens(SETTINGS, clock=lambda: NOW + 901).verify(token)
        with self.assertRaises(ValueError):
            Tokens(replace(SETTINGS, signing_key='different-signing-secret-1234567890123456'),
                   clock=lambda: NOW).verify(token)

    def test_configuration_rejects_wildcard_origins_and_short_keys(self):
        with self.assertRaises(ValueError):
            GatewaySettings((Source('demo', 'long-enough-source-key', ('*',)),), SETTINGS.signing_key)
        with self.assertRaises(ValueError):
            replace(SETTINGS, signing_key='short')
        with self.assertRaises(ValueError):
            replace(SETTINGS, max_batch_size=101)


class PublisherTests(unittest.TestCase):
    def test_only_acknowledged_futures_return_acceptance(self):
        producer = MagicMock()
        good, failed = MagicMock(), MagicMock()
        good.get.return_value = SimpleNamespace(topic=EVENT_TOPIC, partition=2, offset=42)
        failed.get.side_effect = TimeoutError('private broker details')
        producer.send.side_effect = [good, failed]
        factory = MagicMock(return_value=producer)
        publisher = KafkaPublisher('broker:9092', factory)
        outcomes = publisher.publish_many([Record(EVENT_TOPIC, 'u123', {}), Record(EVENT_TOPIC, 'u123', {})])
        self.assertEqual(outcomes, [dict(topic=EVENT_TOPIC, partition=2, offset=42), None])
        self.assertEqual(factory.call_args.kwargs['acks'], 'all')
        self.assertEqual(factory.call_args.kwargs['max_in_flight_requests_per_connection'], 1)
        producer.send.assert_any_call(EVENT_TOPIC, key='u123', value={})

    def test_readiness_queries_real_metadata_and_closes_admin(self):
        admin = MagicMock()
        admin.describe_topics.return_value = [
            dict(topic=topic, error_code=0, partitions=[dict(leader=1)] * 3)
            for topic in (EVENT_TOPIC, QUARANTINE_TOPIC)]
        publisher = KafkaPublisher('broker:9092', admin_factory=MagicMock(return_value=admin))
        self.assertTrue(publisher.ready())
        admin.close.assert_called_once()
        admin.describe_topics.return_value[0]['partitions'][0]['leader'] = -1
        self.assertFalse(publisher.ready())

    def test_enqueue_failure_and_factory_failure_are_not_success(self):
        factory = MagicMock(side_effect=RuntimeError('private details'))
        publisher = KafkaPublisher('broker:9092', factory, admin_factory=factory)
        self.assertEqual(publisher.publish_many([Record(EVENT_TOPIC, 'u', {})]), [None])
        self.assertFalse(publisher.ready())
        producer = MagicMock()
        producer.send.side_effect = RuntimeError('private details')
        publisher = KafkaPublisher('broker:9092', MagicMock(return_value=producer))
        self.assertEqual(publisher.publish_many([Record(EVENT_TOPIC, 'u', {})]), [None])


if __name__ == '__main__':
    unittest.main()
