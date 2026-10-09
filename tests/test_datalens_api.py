from contextlib import contextmanager
from datetime import datetime, timezone
from decimal import Decimal
import os
import unittest
from unittest.mock import MagicMock, patch
from uuid import uuid4

from fastapi.testclient import TestClient
import psycopg2

from datalens.api import create_app, get_service
from datalens.investigation import EvidenceService
from datalens.models import Catalogue, InvestigationRequest, PaymentFreshness, QualityPage

NOW = datetime(2026,1,2,tzinfo=timezone.utc)
BODY = dict(from_utc='2026-01-01T00:00:00Z',until_utc='2026-01-01T00:05:00Z')


def catalogue_fixture():
    return Catalogue(datasets=[dict(name='processed_events',description='audit',owner='pipeline',physical_location='postgres')],
        metrics=[dict(name='payment_revenue',definition='gross payments',source_dataset='payment_revenue_windows',semantics={},definition_version=1)],
        dependencies=[dict(upstream='user-events',downstream='payment_revenue_windows',description='aggregate'),
                      dict(upstream='payment_revenue_windows',downstream='metric:payment_revenue',description='metric')])


class FixtureService(EvidenceService):
    def __init__(self,status='mismatch',quality=None):
        super().__init__({})
        self.status = status
        self.page = quality or QualityPage(results=[],truncated=False,limit=100)
        self.connections = []
        self.token = object()

    @contextmanager
    def snapshot(self):
        yield self.token

    def catalogue(self,conn):
        self.connections.append(conn)
        return catalogue_fixture()

    def compare(self,conn,request):
        self.connections.append(conn)
        row = dict(window_start=datetime(2026,1,1),window_end=datetime(2026,1,1,0,5),
            audit_count=2,audit_revenue=Decimal('0.30'),stored_count=1,stored_revenue=Decimal('0.10'),
            revenue_delta=Decimal('0.20'),finding='mismatch')
        if self.status == 'match':
            row.update(stored_count=2,stored_revenue=Decimal('0.30'),revenue_delta=Decimal('0.00'),finding='match')
        return dict(status=self.status,observed_at=NOW,from_utc=datetime(2026,1,1,tzinfo=timezone.utc),
            until_utc=datetime(2026,1,1,0,5,tzinfo=timezone.utc),currency='MYR',window_seconds=300,
            rows=[] if self.status=='no_data' else [row],limitations='Audit does not prove completeness')

    def freshness(self,conn):
        self.connections.append(conn)
        return PaymentFreshness(observed_at=NOW,payment_count=2,latest_processed_at=NOW,
            latest_ingested_at=NOW,latest_event_timestamp_ms=1767225600000,
            processing_age_seconds=Decimal(0),event_age_seconds=Decimal(86400),recent_payment_count=2)

    def quality(self,conn,*args,**kwargs):
        self.connections.append(conn)
        return self.page


class ApiTests(unittest.TestCase):
    def setUp(self):
        self.env = patch.dict(os.environ,{'DATALENS_API_KEY':''})
        self.env.start()
        self.addCleanup(self.env.stop)
        self.app = create_app()
        self.service = FixtureService()
        self.app.dependency_overrides[get_service] = lambda:self.service
        self.client = TestClient(self.app)
        self.addCleanup(self.client.close)

    def test_investigation_exposes_exact_money_utc_and_evidence_references(self):
        response = self.client.post('/v1/investigations',json=BODY)
        self.assertEqual(response.status_code,200,response.text)
        report = response.json()
        self.assertEqual(report['status'],'discrepancy')
        self.assertEqual(report['comparison']['rows'][0]['audit_revenue'],'0.30')
        self.assertEqual(report['comparison']['rows'][0]['revenue_delta'],'0.20')
        self.assertTrue(report['comparison']['rows'][0]['window_start'].endswith('Z'))
        self.assertIn('comparison.rows.0',report['findings'][0]['evidence_refs'])
        self.assertEqual(len(self.service.connections),4)
        self.assertTrue(all(c is self.service.token for c in self.service.connections))
        self.assertTrue(any('causal' in item for item in report['uncertainty']))

    def test_agreement_does_not_claim_complete_or_healthy(self):
        self.service.status='match'
        report=self.client.post('/v1/investigations',json=BODY).json()
        self.assertEqual(report['status'],'no_discrepancy_found')
        self.assertIn('completeness remains unverified',report['findings'][0]['message'])

    def test_no_data_is_insufficient_evidence(self):
        self.service.status='no_data'
        self.assertEqual(self.client.post('/v1/investigations',json=BODY).json()['status'],'insufficient_evidence')

    def test_bad_ranges_extra_fields_wrong_metric_and_boolean_window_rejected(self):
        for update in [dict(from_utc='2026-01-01'),dict(currency="MYR'; DROP"),dict(window_seconds=True),
                       dict(metric='completed_orders'),dict(sql='SELECT 1'),dict(quality_limit=1001),
                       dict(until_utc='9999-01-01T00:00:00Z')]:
            with self.subTest(update=update):
                self.assertEqual(self.client.post('/v1/investigations',json=dict(BODY,**update)).status_code,422)
        self.assertEqual(self.service.connections,[])

    def test_quality_limits_and_range_validation(self):
        self.assertEqual(self.client.get('/v1/quality',params=dict(BODY,limit=1001)).status_code,422)
        self.assertEqual(self.client.get('/v1/quality',params=dict(BODY,from_utc='2026-01-01')).status_code,422)
        self.assertEqual(self.client.get('/v1/quality',params=BODY).status_code,200)

    def test_database_errors_are_sanitized_and_not_healthy(self):
        with patch.object(self.service,'catalogue',side_effect=psycopg2.OperationalError('secret-password-host')):
            response=self.client.get('/v1/catalogue')
            self.assertEqual(response.status_code,503)
            self.assertNotIn('secret-password-host',response.text)
        self.assertEqual(self.client.get('/health/live').status_code,200)

    def test_optional_key_protects_data_and_readiness(self):
        with patch.dict(os.environ,{'DATALENS_API_KEY':'test-secret'}):
            self.assertEqual(self.client.get('/v1/catalogue').status_code,401)
            self.assertEqual(self.client.get('/v1/health/ready').status_code,401)
            self.assertEqual(self.client.get('/v1/catalogue',headers={'X-DataLens-Key':'test-secret'}).status_code,200)
            self.assertEqual(self.client.get('/health/live').status_code,200)

    def test_dependency_traversal_handles_cycles_and_unknown_nodes(self):
        catalogue=catalogue_fixture()
        from datalens.models import Dependency
        catalogue.dependencies.append(Dependency(upstream='metric:payment_revenue',downstream='user-events',description='cycle'))
        result=EvidenceService.dependencies(catalogue,'metric:payment_revenue','upstream')
        self.assertEqual(len(result.edges),3)
        self.assertEqual(self.client.get('/v1/dependencies',params={'node':'unknown'}).status_code,404)

    def test_truncated_quality_is_explicit(self):
        self.service.page=QualityPage(results=[],truncated=True,limit=1)
        report=self.client.post('/v1/investigations',json=BODY).json()
        self.assertTrue(report['historical_quality']['truncated'])
        self.assertIn('quality_sample_truncated',[f['code'] for f in report['findings']])

    def test_openapi_and_comparison_contract(self):
        schema=self.client.get('/openapi.json').json()
        self.assertIn('Investigation',schema['components']['schemas'])
        self.assertEqual(self.client.post('/v1/revenue/compare',json=BODY).status_code,200)
        self.assertEqual(self.client.get('/v1/payments/freshness').status_code,200)

    def test_shared_comparison_connection_is_not_closed(self):
        from datalens import evidence
        conn=MagicMock()
        cur=conn.cursor.return_value.__enter__.return_value
        cur.fetchone.return_value={'observed_at':NOW}
        cur.fetchall.return_value=[]
        evidence.compare_revenue({},**dict(start=BODY['from_utc'],end=BODY['until_utc']),connection=conn)
        conn.close.assert_not_called()


if __name__ == '__main__':
    unittest.main()
