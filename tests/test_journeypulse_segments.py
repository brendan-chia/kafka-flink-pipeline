"""Deterministic clock/boundary tests; database replay tests are a separate probe."""
from datetime import datetime, timedelta, timezone
import unittest
from unittest.mock import MagicMock

from fastapi.testclient import TestClient
import psycopg2

from journeypulse.auth import Tokens
from journeypulse.customer_api import create_app
from journeypulse.profiles import ProfileStore
from journeypulse.segments import SegmentSettings, evaluate, register_rules, targeting_response
from test_journeypulse_profiles import SETTINGS

NOW = datetime(2026, 10, 12, 8, tzinfo=timezone.utc)
SHORT = SegmentSettings(window_seconds=10, abandon_seconds=10, poll_seconds=1, stale_seconds=10)


class RuleTests(unittest.TestCase):
    def test_threshold_entry_and_silent_expiry(self):
        views = [NOW - timedelta(seconds=i) for i in range(5)]
        before, _ = evaluate(NOW, views[:4], [], 0, SHORT)
        self.assertFalse(before['active_browser']['member'])
        entered, due = evaluate(NOW, views, [], 0, SHORT)
        self.assertTrue(entered['active_browser']['member'])
        self.assertEqual(entered['active_browser']['valid_until'], NOW + timedelta(seconds=6))
        expired, _ = evaluate(NOW+timedelta(seconds=6), views, [], 0, SHORT)
        self.assertFalse(expired['active_browser']['member'])
        self.assertLessEqual(due, NOW+timedelta(seconds=1))

    def test_future_views_do_not_enter_early(self):
        results, _ = evaluate(NOW, [NOW+timedelta(seconds=1)]*5, [], 0, SHORT)
        self.assertFalse(results['active_browser']['member'])

    def test_separate_carts_delayed_completion_and_silent_deadline(self):
        carts = [dict(latest_add_at=NOW, completed_at=None) for _ in range(2)]
        before, _ = evaluate(NOW+timedelta(seconds=9), [], carts, 0, SHORT)
        self.assertFalse(before['abandoned_cart']['member'])
        after, _ = evaluate(NOW+timedelta(seconds=10), [], carts, 0, SHORT)
        self.assertEqual(after['abandoned_cart']['evidence']['count'], 2)
        carts[0]['completed_at'] = NOW+timedelta(seconds=1)
        corrected, _ = evaluate(NOW+timedelta(seconds=11), [], carts, 1, SHORT)
        self.assertEqual(corrected['abandoned_cart']['evidence']['count'], 1)
        carts[1]['completed_at'] = NOW+timedelta(seconds=2)
        resolved, _ = evaluate(NOW+timedelta(seconds=12), [], carts, 2, SHORT)
        self.assertFalse(resolved['abandoned_cart']['member'])

    def test_latest_add_deadline_and_downtime_catch_up(self):
        carts = [dict(latest_add_at=NOW+timedelta(seconds=5), completed_at=None)]
        early, _ = evaluate(NOW+timedelta(seconds=10), [], carts, 0, SHORT)
        self.assertFalse(early['abandoned_cart']['member'])
        recovered, _ = evaluate(NOW+timedelta(days=1), [NOW]*5, carts, 0, SHORT)
        self.assertTrue(recovered['abandoned_cart']['member'])
        self.assertFalse(recovered['active_browser']['member'])

    def test_repeat_customer_uses_distinct_order_total(self):
        for orders in (0, 2, 3, 10):
            results, _ = evaluate(NOW, [], [], orders, SHORT)
            self.assertEqual(results['repeat_customer']['member'], orders >= 3)

    def test_versions_freeze_thresholds_and_registration_rejects_changes(self):
        self.assertEqual(SegmentSettings().version, '1')
        self.assertNotEqual(SHORT.version, '1')
        self.assertEqual(SHORT.version, SegmentSettings(10, 10, 2, 10).version)
        cursor = MagicMock()
        cursor.fetchone.return_value = ({'changed': True},)
        with self.assertRaises(ValueError):
            register_rules(cursor, SHORT, create=True)
        for kwargs in ({'window_seconds': 0}, {'poll_seconds': 20}, {'abandon_seconds': True}):
            with self.assertRaises(ValueError):
                SegmentSettings(**kwargs)


class TargetingTests(unittest.TestCase):
    def setUp(self):
        self.row = dict(user_id='u123', marketing_consent=True, onboarded_at=NOW,
            consent_updated_at=NOW, served_at=NOW, evaluated_at=NOW,
            next_evaluation_at=NOW+timedelta(seconds=1), rule_version=SHORT.version)
        results, _ = evaluate(NOW, [NOW]*5, [], 0, SHORT)
        self.memberships = [dict(segment=name, evaluated_at=NOW, **r) for name, r in results.items()]

    def response(self):
        return targeting_response(self.row, self.memberships, SHORT)

    def test_consent_revoke_and_restore_without_behavior(self):
        self.assertTrue(self.response()['eligible'])
        self.row['marketing_consent'] = False
        self.assertFalse(self.response()['eligible'])
        self.assertIn('marketing_consent_required', self.response()['segments']['active_browser']['reasons'])
        self.row['marketing_consent'] = True
        self.assertTrue(self.response()['eligible'])
        self.row['onboarded_at'] = None
        self.assertFalse(self.response()['eligible'])

    def test_stale_unevaluated_and_expired_fail_closed(self):
        self.row['served_at'] = NOW+timedelta(seconds=10)
        self.assertFalse(self.response()['segments']['active_browser']['member'])
        self.assertIn('active_browser_window_expired', self.response()['segments']['active_browser']['reasons'])
        self.assertFalse(self.response()['eligible'])
        self.row['served_at'] += timedelta(seconds=1)
        self.assertEqual(self.response()['freshness']['status'], 'stale')
        self.row['evaluated_at'] = None
        self.assertEqual(self.response()['freshness']['status'], 'unevaluated')
        self.assertFalse(self.response()['eligible'])
        self.row['evaluated_at'] = NOW
        self.row['rule_version'] = 'old'
        self.assertEqual(self.response()['freshness']['status'], 'unevaluated')
        self.assertIsNone(self.response()['segments']['active_browser']['member'])

    def test_partial_membership_is_unevaluated(self):
        self.memberships.pop()
        self.assertEqual(self.response()['freshness']['status'], 'unevaluated')

    def test_api_preserves_authorization_and_sanitizes_unavailable_state(self):
        store = MagicMock()
        store.targeting.side_effect = lambda _: self.response()
        token = Tokens(SETTINGS, audience='journeypulse-customers').issue('demo-app', 'u123', 's456')[0]
        headers = {'Authorization': 'Bearer '+token}
        with TestClient(create_app(SETTINGS, store)) as client:
            path = '/v1/customers/u123/targeting'
            self.assertEqual(client.get(path).status_code, 401)
            tracking = Tokens(SETTINGS).issue('demo-app', 'u123', 's456')[0]
            self.assertEqual(client.get(path, headers={'Authorization': 'Bearer '+tracking}).status_code, 401)
            self.assertEqual(client.get('/v1/customers/other/targeting', headers=headers).status_code, 403)
            self.assertEqual(client.get(path, headers={**headers, 'Origin': 'https://other.example'}).status_code, 403)
            store.targeting.assert_not_called()
            response = client.get(path, headers=headers)
            self.assertEqual(response.status_code, 200)
            self.assertEqual(response.headers['cache-control'], 'no-store')
            self.assertTrue(response.json()['eligible'])
            self.row['marketing_consent'] = False
            self.assertFalse(client.get(path, headers=headers).json()['eligible'])
            store.targeting.side_effect = None
            store.targeting.return_value = None
            self.assertEqual(client.get(path, headers=headers).status_code, 404)
            store.targeting.side_effect = psycopg2.OperationalError('private credentials')
            response = client.get(path, headers=headers)
            self.assertEqual(response.status_code, 503)
            self.assertEqual(response.json()['detail'], 'database_unavailable')
            self.assertNotIn('private', response.text)
            metrics = client.get('/metrics').text
            self.assertIn('journeypulse_targeting_requests_total{result="current"}', metrics)
            self.assertNotIn('u123', metrics)


class SchedulerTests(unittest.TestCase):
    def test_restart_selects_persisted_deadlines_with_shared_locks(self):
        connection = MagicMock()
        cursor = connection.cursor.return_value.__enter__.return_value
        cursor.fetchall.return_value = []
        store = ProfileStore(connect=lambda: connection, segment_settings=SHORT)
        self.assertEqual(store.evaluate_due(), 0)
        sql, params = cursor.execute.call_args.args
        self.assertIn('next_evaluation_at <= clock_timestamp()', sql)
        self.assertIn('FOR UPDATE OF b SKIP LOCKED', sql)
        self.assertEqual(params, (SHORT.version, None, None, SHORT.batch_size))
        connection.close.assert_called_once()


if __name__ == '__main__':
    unittest.main()
