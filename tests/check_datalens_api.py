"""Real PostgreSQL + HTTP-layer investigation check in a disposable test database."""
import argparse
import os
from pathlib import Path
import subprocess
import sys
from uuid import uuid4

import psycopg2
from psycopg2 import sql
from fastapi.testclient import TestClient
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from datalens import evidence
from datalens.api import create_app, get_service, get_workflow
from datalens.workflow import InvestigationWorkflow
from datalens.llm import ModelConfig
from datalens.assistant_models import ModelAssessment
import json
from datalens.investigation import EvidenceService


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--container',help='Resolve host port of a disposable PostgreSQL container')
    args=parser.parse_args()
    params=evidence.connection_params()
    if args.container:
        port=subprocess.check_output(['docker','port',args.container,'5432/tcp'],text=True).splitlines()[0].rsplit(':',1)[1]
        params.update(host='127.0.0.1',port=int(port))
    admin=psycopg2.connect(**dict(params,connect_timeout=5))
    admin.autocommit=True
    database='datalens_api_test_' + uuid4().hex
    created=False
    fixture=None
    try:
        with admin.cursor() as cur:
            cur.execute(sql.SQL('CREATE DATABASE {}').format(sql.Identifier(database)))
        created=True
        test_params=dict(params,dbname=database)
        fixture=psycopg2.connect(**test_params)
        root=Path(__file__).resolve().parents[1]
        with fixture:
            with fixture.cursor() as cur:
                cur.execute((root/'sql/init.sql').read_text())
                migration=(root/'sql/migrations/004_datalens_evidence.sql').read_text().replace('BEGIN;','').replace('COMMIT;','')
                cur.execute(migration)
                cur.execute("""INSERT INTO processed_events
                    (event_id,user_id,event_type,amount,currency,event_timestamp_ms,ingested_at,processed_at,source_topic,source_partition,source_offset)
                    VALUES ('a','u','payment',0.10,'MYR',1767225600000,'2026-01-01','2026-01-01','test',0,0),
                           ('b','u','payment',0.20,'MYR',1767225601000,'2026-01-02','2026-01-02','test',0,1),
                           ('usd','u','payment',99,'USD',1767225600000,'2026-01-01','2026-01-01','test',0,2);
                    INSERT INTO payment_revenue_windows VALUES ('2026-01-01','2026-01-01 00:05','MYR',1,0.10);
                    INSERT INTO datalens.quality_results VALUES
                    ('00000000-0000-0000-0000-000000000001','2026-01-01T00:04:00Z','source_validation_window','user-events','partial','2026-01-01T00:00:00Z','2026-01-01T00:04:00Z','{}'),
                    ('00000000-0000-0000-0000-000000000002','2026-01-01T00:03:00Z','payment_freshness','processed_events','observed','2026-01-01T00:03:00Z','2026-01-01T00:03:00Z','{}'),
                    ('00000000-0000-0000-0000-000000000003','2026-01-01T00:02:00Z','payment_freshness','processed_events','observed','2026-01-01T00:02:00Z','2026-01-01T00:02:00Z','{}');""")
        service=EvidenceService(test_params)
        with service.snapshot() as conn:
            with conn.cursor() as cur:
                cur.execute('SHOW transaction_read_only')
                assert cur.fetchone()[0]=='on'
                try:
                    cur.execute('DELETE FROM processed_events')
                except psycopg2.errors.ReadOnlySqlTransaction:
                    conn.rollback()
                else:
                    raise AssertionError('Diagnostic connection allowed business-data writes')
        class FixtureModel:
            config = ModelConfig('fixture-only', 'fixture-model')
            calls = 0

            def assess(self, payload):
                self.calls += 1
                context = json.loads(payload)
                ref = next(c['ref'] for c in context['citations'] if c['kind']=='runbook')
                return ModelAssessment(hypotheses=[dict(cause='late_arrival', evidence_refs=['comparison',ref])])

        model=FixtureModel()
        app=create_app()
        app.dependency_overrides[get_service]=lambda:service
        app.dependency_overrides[get_workflow]=lambda:InvestigationWorkflow(service,model)
        headers={'X-DataLens-Key':os.environ.get('DATALENS_API_KEY','')}
        with TestClient(app,headers=headers) as client:
            assert client.get('/v1/health/ready').status_code==200
            assert len(client.get('/v1/catalogue').json()['metrics'])==2
            body=dict(from_utc='2026-01-01T00:00:00Z',until_utc='2026-01-01T00:05:00Z',quality_limit=2)
            response=client.post('/v1/investigations',json=body)
            assert response.status_code==200,response.text
            report=response.json()
            assert report['status']=='discrepancy',report
            assert report['comparison']['rows'][0]['audit_revenue']=='0.30',report
            assert report['comparison']['rows'][0]['stored_revenue']=='0.10',report
            assert len(report['historical_quality']['results'])==2
            assert report['historical_quality']['truncated'] is True
            assistant=client.post('/v1/assistant/investigations',json=dict(body,question='Explain the revenue discrepancy'))
            assert assistant.status_code==200,assistant.text
            answer=assistant.json()
            assert answer['model_status']=='completed',answer
            assert model.calls==1
            assert answer['evidence']['comparison']['rows'][0]['audit_revenue']=='0.30'
            assert answer['suspected_causes']
            refs={c['ref'] for c in answer['citations']}
            for field in ('observations','suspected_causes','missing_evidence','uncertainty','manual_next_steps'):
                assert all(set(item['evidence_refs']) <= refs for item in answer[field])
            assert client.get('/v1/runbooks',params={'q':'late payment windows'}).json()['results']
            # Instant observations at start are included; at end are excluded.
            history=client.get('/v1/quality',params=dict(from_utc='2026-01-01T00:02:00Z',until_utc='2026-01-01T00:03:00Z',check_name='payment_freshness'))
            assert history.status_code==200,history.text
            assert len(history.json()['results'])==1,history.text
            compare=client.post('/v1/revenue/compare',json={k:v for k,v in body.items() if k!='quality_limit'})
            assert compare.status_code==200,compare.text
            dependencies=client.get('/v1/dependencies',params={'node':'metric:payment_revenue'})
            assert len(dependencies.json()['edges'])==2,dependencies.text
        with fixture.cursor() as cur:
            cur.execute('SELECT payment_count,revenue FROM payment_revenue_windows')
            assert cur.fetchone()==(1,__import__('decimal').Decimal('0.10'))
            cur.execute('SELECT count(*) FROM datalens.quality_results')
            assert cur.fetchone()[0]==3
        print('PASS: real PostgreSQL evidence + bounded assistant HTTP contracts, exact money, history overlap/truncation, lineage, enforced read-only access and unchanged business/evidence rows.')
    finally:
        if fixture is not None:
            fixture.close()
        if created:
            with admin.cursor() as cur:
                # This name is generated locally and refers only to this test's database.
                cur.execute(sql.SQL('DROP DATABASE {} WITH (FORCE)').format(sql.Identifier(database)))
        admin.close()


if __name__ == '__main__':
    main()
