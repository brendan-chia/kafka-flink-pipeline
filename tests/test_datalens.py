from datetime import datetime, timezone
from decimal import Decimal
import json
import math
import unittest
from unittest.mock import MagicMock, patch

from datalens import evidence
from monitoring import metrics


class EvidenceTests(unittest.TestCase):
    def test_range_rejects_ambiguous_days_currency_injection_and_open_windows(self):
        for start, end, currency, seconds in [
            ('2026-01-01','2026-01-02','MYR',300),
            ('2026-01-01T00:00:01Z','2026-01-02T00:00:00Z','MYR',300),
            ('2026-01-01T00:00:00Z','2026-01-02T00:00:00Z',"MYR'; DROP",300),
            ('2026-01-01T00:00:00Z','2026-01-09T00:00:00Z','MYR',300),
            ('2026-01-01T00:00:00Z','9999-01-01T00:00:00Z','MYR',300),
            ('2026-01-01T00:00:00Z','2026-01-02T00:00:00Z','MYR',True)]:
            with self.subTest(start=start, currency=currency, seconds=seconds):
                with self.assertRaises(ValueError):
                    evidence.validate_range(start,end,currency,seconds)

    def test_diagnostics_request_readonly_repeatable_snapshot(self):
        conn = MagicMock()
        with patch.object(evidence.psycopg2, 'connect', return_value=conn):
            evidence.connect({})
        conn.set_session.assert_called_once_with(readonly=True, isolation_level='REPEATABLE READ')

    def comparison(self, rows):
        conn = MagicMock()
        cur = conn.cursor.return_value.__enter__.return_value
        cur.fetchone.return_value = {'observed_at':datetime.now(timezone.utc)}
        cur.fetchall.return_value = rows
        with patch.object(evidence, 'connect', return_value=conn):
            result = evidence.compare_revenue({},'2026-01-01T00:00:00Z','2026-01-01T00:05:00Z')
        conn.close.assert_called_once()
        return result

    def row(self, **updates):
        row = dict(window_start=datetime(2026,1,1),window_end=datetime(2026,1,1,0,5),
                   audit_count=1,audit_revenue=Decimal('0.10'),stored_count=1,stored_revenue=Decimal('0.10'))
        row.update(updates)
        return row

    def test_missing_zero_value_aggregate_is_not_reported_as_match(self):
        report = self.comparison([self.row(window_end=None,stored_count=None,stored_revenue=None,audit_revenue=Decimal(0))])
        self.assertEqual(report['status'],'mismatch')
        self.assertEqual(report['rows'][0]['finding'],'missing_aggregate')

    def test_exact_money_counts_and_incompatible_windows(self):
        report = self.comparison([self.row(audit_revenue=Decimal('0.30'),stored_revenue=Decimal('0.10')),
                                  self.row(stored_count=2), self.row(window_end=datetime(2026,1,1,0,10))])
        self.assertEqual([r['finding'] for r in report['rows']],['mismatch','mismatch','incompatible_window_configuration'])
        self.assertEqual(report['rows'][0]['revenue_delta'],Decimal('0.20'))
        self.assertIn('0.20',json.dumps(report,default=evidence.json_value))

    def test_no_data_is_unknown_and_stored_only_group_is_discrepancy(self):
        self.assertEqual(self.comparison([])['status'],'no_data')
        self.assertEqual(self.comparison([self.row(audit_count=None,audit_revenue=None)])['rows'][0]['finding'], 'aggregate_without_audit')
        self.assertEqual(self.comparison([self.row()])['status'],'match')

    def test_source_quality_preserves_partial_coverage_and_empty_uncertainty(self):
        with patch.object(evidence,'persist_result',return_value='id') as persist:
            evidence.record_source_quality({},2,1,{'INVALID_AMOUNT':1},1000,990,{})
            self.assertEqual(persist.call_args.args[3],'partial')
            self.assertEqual(persist.call_args.args[-1]['invalid_fraction'],1/3)
            evidence.record_source_quality({},0,0,{},1000,600,{})
            self.assertEqual(persist.call_args.args[3],'unknown')
            self.assertIsNone(persist.call_args.args[-1]['invalid_fraction'])

    def test_persistence_failure_does_not_propagate_to_source_observation(self):
        with self.assertLogs(metrics.logger,level='WARNING'):
            metrics.try_persist('test',MagicMock(side_effect=RuntimeError('unavailable')))
        self.assertEqual(metrics.evidence_success.labels(check='test')._value.get(),0)

    def test_fresh_food_activity_does_not_make_payment_freshness_healthy(self):
        row = dict(processing_age_seconds=120,event_age_seconds=180,recent_payment_count=0)
        with patch.object(evidence,'payment_freshness',return_value=row):
            metrics.collect_payment_freshness({})
        self.assertEqual(metrics.payment_freshness._value.get(),120)
        with patch.object(evidence,'payment_freshness',return_value=dict(row,processing_age_seconds=None,event_age_seconds=None)):
            metrics.collect_payment_freshness({})
        self.assertTrue(math.isnan(metrics.payment_freshness._value.get()))


if __name__ == '__main__':
    unittest.main()
