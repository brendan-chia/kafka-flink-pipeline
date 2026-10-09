import base64
from datetime import datetime, timedelta, timezone
import importlib.util
import os
from pathlib import Path
import unittest
from unittest.mock import patch

import lakehouse_config
from lakehouse_ops import TrinoClient, canonical_cte, replay_sql, validated_range

spec = importlib.util.spec_from_file_location('lakehouse_cli', Path(__file__).resolve().parents[1] / 'scripts/lakehouse.py')
cli = importlib.util.module_from_spec(spec)
spec.loader.exec_module(cli)


class LakehouseTests(unittest.TestCase):
    def test_integration_is_opt_in(self):
        with patch.dict(os.environ, {'LAKEHOUSE_ENABLED': '0'}):
            self.assertFalse(lakehouse_config.enabled())
        with patch.dict(os.environ, {'LAKEHOUSE_ENABLED': 'invalid'}):
            with self.assertRaises(ValueError):
                lakehouse_config.enabled()

    def test_config_escapes_sql_literals_and_rejects_identifiers(self):
        self.assertEqual(lakehouse_config.properties_sql({'key': "a'b"}), "'key'='a''b'")
        with patch.dict(os.environ, {'LAKEHOUSE_NAMESPACE': 'analytics;DROP TABLE x'}):
            with self.assertRaises(ValueError):
                lakehouse_config.namespace()

    def test_replay_retains_null_empty_and_invalid_utf8(self):
        for payload in (b'', b'\xff', b'{broken'):
            row = {'raw_payload_base64': base64.b64encode(payload).decode(), 'payload_is_null': False}
            self.assertEqual(cli.replay_payload(row), payload)
        self.assertIsNone(cli.replay_payload({'raw_payload_base64': '', 'payload_is_null': True}))
        with self.assertRaises(ValueError):
            cli.replay_payload({'raw_payload_base64': 'bad!', 'payload_is_null': False})

    def test_range_requires_utc_alignment_and_completed_windows(self):
        start = datetime(2020, 1, 1, tzinfo=timezone.utc)
        self.assertEqual(validated_range(start.isoformat(), (start+timedelta(minutes=5)).isoformat()),
                         (1577836800000,1577837100000))
        for first, last in [('2020-01-01T00:00:01Z','2020-01-01T00:05:00Z'),
                            ('2020-01-01T00:00:00','2020-01-01T00:05:00Z'),
                            ('2020-01-01T00:05:00Z','2020-01-01T00:00:00Z'),
                            ('2099-01-01T00:00:00Z','2099-01-01T00:05:00Z')]:
            with self.assertRaises(ValueError):
                validated_range(first, last)

    def test_snapshot_ids_cannot_inject_sql(self):
        with self.assertRaises(ValueError):
            canonical_cte('1;DROP TABLE x')
        with self.assertRaises(ValueError):
            replay_sql(-1,0,1000,10)

    def test_zero_snapshot_is_rejected_instead_of_selecting_current_snapshot(self):
        client = TrinoClient()
        args = type('Args', (), dict(from_utc='2020-01-01T00:00:00Z',
            until_utc='2020-01-01T00:05:00Z', snapshot=0,window_seconds=300))()
        with patch.object(client, 'current_snapshot') as current:
            with self.assertRaises(ValueError):
                cli.reconcile(client,args)
            current.assert_not_called()

    def test_trino_pagination_and_decimal_values(self):
        client = TrinoClient()
        responses = [
            {'columns':[{'name':'revenue'}], 'data':[['10.10']], 'nextUri':'http://localhost:18080/next'},
            {'data':[['0.20']]}]
        with patch.object(client, 'request', side_effect=responses):
            self.assertEqual(client.query('SELECT'), [{'revenue':'10.10'},{'revenue':'0.20'}])

    def test_trino_error_cancels_continuation(self):
        client = TrinoClient()
        with patch.object(client, 'request', side_effect=[
                {'error':{'message':'failed'},'nextUri':'http://localhost:18080/next'}, {}]) as request:
            with self.assertRaisesRegex(RuntimeError, 'failed'):
                client.query('SELECT')
            self.assertEqual(request.call_args.args[1], 'DELETE')

    def test_trino_limits_results_and_rejects_cross_origin(self):
        client = TrinoClient()
        with patch.object(client, 'request', return_value={'data':[[1],[2]]}):
            with self.assertRaisesRegex(RuntimeError, 'exceeded'):
                client.query('SELECT', max_rows=1)
        with self.assertRaisesRegex(RuntimeError, 'origin'):
            client.request('http://other.example/next')

    def test_replay_to_source_topic_is_rejected_before_producer(self):
        client = TrinoClient()
        args = type('Args', (), dict(from_utc='2020-01-01T00:00:00Z',
            until_utc='2020-01-01T00:01:00Z', snapshot=123,max_records=10,
            target_topic='source',execute=True))()
        rows = [{'source_topic':'source'}]
        with patch.object(client, 'query', return_value=rows):
            with self.assertRaisesRegex(ValueError, 'differ'):
                cli.replay(client, args)


if __name__ == '__main__':
    unittest.main()
