import importlib.util
import math
from pathlib import Path
import random
import unittest
from decimal import Decimal
from unittest.mock import MagicMock, patch

from monitoring import metrics

spec = importlib.util.spec_from_file_location('benchmark', Path(__file__).resolve().parents[1] / 'scripts/benchmark_pipeline.py')
benchmark = importlib.util.module_from_spec(spec)
spec.loader.exec_module(benchmark)


class QualityTests(unittest.TestCase):
    def test_window_expires_out_of_order_records_and_counts_duplicates(self):
        window = metrics.QualityWindow()
        for timestamp, reason in [(999, None), (700, 'MALFORMED_JSON'),
                                  (998, 'MULTIPLE_ERRORS'), (999, None), (600, None)]:
            window.add(timestamp, reason)
        valid, invalid, reasons = window.snapshot(1000)
        self.assertEqual((valid, invalid), (2, 1))
        self.assertEqual(reasons, {'MULTIPLE_ERRORS': 1})
        self.assertEqual(window.snapshot(1300), (0, 0, {}))

    def test_empty_window_is_not_reported_healthy(self):
        metrics.QualityWindow().expose(1000)
        self.assertTrue(math.isnan(metrics.invalid_fraction._value.get()))

    def test_database_failure_closes_connection(self):
        conn = MagicMock()
        conn.cursor.return_value.__enter__.return_value.execute.side_effect = RuntimeError('unavailable')
        with patch.object(metrics.psycopg2, 'connect', return_value=conn):
            with self.assertRaises(RuntimeError):
                metrics.collect_postgres({})
        conn.close.assert_called_once()

    def test_dlq_failure_closes_consumer(self):
        consumer = MagicMock()
        consumer.partitions_for_topic.return_value = {0}
        consumer.end_offsets.side_effect = RuntimeError('timeout')
        with patch.object(metrics, 'new_consumer', return_value=consumer):
            with self.assertRaises(RuntimeError):
                metrics.dlq_offset('localhost', 'dlq')
        consumer.close.assert_called_once()


class BenchmarkTests(unittest.TestCase):
    def test_seed_reproduces_workload(self):
        a, b = random.Random(42), random.Random(42)
        self.assertEqual([benchmark.fixture('run', i, a, 1000) for i in range(10)],
                         [benchmark.fixture('run', i, b, 1000) for i in range(10)])

    def test_reconciliation_detects_missing_duplicate_and_corrupt_rows(self):
        event = benchmark.fixture('run', 0, random.Random(42), 1000)
        missing = dict(event, event_id='missing')
        expected = {event['event_id']: event, 'missing': missing}
        row = (event['event_id'], Decimal(str(event['amount'])), 'MYR', 1000, event['event_type'])
        corrupt = (event['event_id'], Decimal('0'), 'USD', 1001, 'payment')
        result = benchmark.reconcile(expected, [row, corrupt])
        self.assertEqual(result['missing_event_ids'], ['missing'])
        self.assertEqual(result['duplicate_event_ids'], [event['event_id']])
        self.assertEqual(result['mismatched_event_ids'], [event['event_id']])

    def test_percentiles_and_empty_population(self):
        self.assertIsNone(benchmark.percentile([], 0.95))
        self.assertEqual(benchmark.percentile([10, 0], 0.95), 9.5)


if __name__ == '__main__':
    unittest.main()
