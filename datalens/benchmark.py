"""Fixed phase-four corpus and evaluation. Offline mode never calls external services."""
from contextlib import contextmanager
from datetime import datetime, timezone
from decimal import Decimal
from hashlib import sha256
import json
import os
from pathlib import Path
from time import perf_counter
from unittest.mock import patch
from uuid import UUID

from datalens import evidence
from datalens.assistant_models import AssistantRequest
from datalens.investigation import EvidenceService
from datalens.llm import ModelConfig, OpenAIModel
from datalens.models import Catalogue, PaymentFreshness, QualityPage
from datalens.retrieval import DOCUMENTS
from datalens.workflow import CAUSES, InvestigationWorkflow
from event_contract import decode_event

ROOT = Path(__file__).resolve().parents[1]
CORPUS = ROOT / 'tests/fixtures/datalens_benchmark.json'
NOW = datetime(2026, 1, 2, tzinfo=timezone.utc)
START = datetime(2026, 1, 1, tzinfo=timezone.utc)
END = datetime(2026, 1, 1, 0, 5, tzinfo=timezone.utc)


def load_cases():
    return json.loads(CORPUS.read_text(encoding='utf-8'))['cases']


def audit_rows(case):
    unique, rejected = {}, []
    for delivery in case['deliveries']:
        row = decode_event(delivery['payload'].encode(), delivery['received_timestamp_ms'])
        if row[7] is not None:
            rejected.append(row[7])
        elif delivery.get('processed', True):
            if row[0] in unique and unique[row[0]] != row:
                raise ValueError('Benchmark IDs must be immutable')
            unique[row[0]] = row
    return list(unique.values()), rejected


def quality_page(case):
    return QualityPage(results=case['quality'], truncated=False, limit=100)


class OfflineService(EvidenceService):
    """Use production investigation/classification with fixture-backed query results.

    Does not execute PostgreSQL SQL; --postgres evaluates that separately.
    """
    def __init__(self, case):
        super().__init__({})
        self.case, self.calls = case, []

    @contextmanager
    def snapshot(self):
        self.calls.append('snapshot')
        yield self

    def catalogue(self, conn):
        self.calls.append('catalogue')
        return Catalogue(datasets=[], metrics=[dict(name='payment_revenue',
            definition='Sum of valid unique payments by UTC event time and currency.',
            source_dataset='payment_revenue_windows', semantics={'identity':'immutable event_id'},
            definition_version=1)], dependencies=[dict(upstream='user-events',
                downstream='payment_revenue_windows', description='validated aggregate'),
                dict(upstream='payment_revenue_windows', downstream='metric:payment_revenue',
                     description='stored metric')])

    def compare(self, conn, request):
        self.calls.append('compare')
        audit, _ = audit_rows(self.case)
        stored = self.case['stored']
        rows = []
        if audit or stored is not None:
            rows.append(dict(window_start=START.replace(tzinfo=None),
                window_end=END.replace(tzinfo=None) if stored is not None else None,
                audit_count=len(audit) if audit else None,
                audit_revenue=sum((r[4] for r in audit), Decimal('0.00')) if audit else None,
                stored_count=stored['count'] if stored is not None else None,
                stored_revenue=Decimal(stored['revenue']) if stored is not None else None))

        class Cursor:
            def __enter__(self): return self
            def __exit__(self, *args): pass
            def execute(self, *args): pass
            def fetchone(self): return {'observed_at': NOW}
            def fetchall(self): return rows
        class Connection:
            def cursor(self, **kwargs): return Cursor()
        return evidence.compare_revenue({}, request.from_utc, request.until_utc,
            request.currency, request.window_seconds, connection=Connection())

    def freshness(self, conn):
        self.calls.append('freshness')
        audit, _ = audit_rows(self.case)
        return PaymentFreshness(observed_at=NOW, payment_count=len(audit),
            latest_processed_at=START if audit else None, latest_ingested_at=START if audit else None,
            latest_event_timestamp_ms=max((r[3] for r in audit), default=None),
            processing_age_seconds=Decimal(86400) if audit else None,
            event_age_seconds=Decimal(86400) if audit else None, recent_payment_count=0)

    def quality(self, conn, *args, **kwargs):
        self.calls.append('quality')
        return quality_page(self.case)


class NoModel:
    config = ModelConfig('', '')
    usage = None
    def assess(self, payload):
        raise AssertionError('Offline evaluation attempted a model call')


def evaluate(case, report, service_calls, elapsed_ms, usage=None, rates=None, live=False):
    """Independent literal expectations, exact decimal comparison, provenance and bounds."""
    expected = case['expected']
    rows = report.evidence.comparison.rows
    row = rows[0] if rows else None
    numbers = len(rows) == (0 if expected['finding'] is None else 1) and all((getattr(row, key) if row else None) ==
                  (Decimal(value) if key.endswith(('revenue', 'delta')) and value is not None else value)
                  for key, value in expected['numbers'].items())
    citations = {c.ref: c for c in report.citations}
    statements = sum((getattr(report, key) for key in
        ('observations','suspected_causes','missing_evidence','uncertainty','manual_next_steps')), [])
    provenance = all(s.evidence_refs and all(r in citations for r in s.evidence_refs) for s in statements)
    # Resolving a reference alone is insufficient: detail citations must match returned data.
    for ref, citation in citations.items():
        if ref.startswith('comparison.rows.'):
            index = int(ref.rsplit('.', 1)[1])
            provenance &= json.loads(citation.content) == rows[index].model_dump(mode='json')
        if citation.kind == 'runbook':
            if citation.source not in DOCUMENTS:
                provenance = False
                continue
            path = ROOT / citation.source
            provenance &= path.is_file() and citation.version == sha256(path.read_bytes()).hexdigest()[:16]
            if path.is_file():
                lines = path.read_text(encoding='utf-8').splitlines()
                provenance &= citation.content == '\n'.join(lines[citation.line_start-1:citation.line_end])[:2400]
    selected = [next(k for k, v in CAUSES.items() if v == s.message) for s in report.suspected_causes]
    support = provenance and set(selected) <= set(expected['allowed_causes'])
    for statement in report.suspected_causes:
        support &= any(citations[r].kind == 'runbook' for r in statement.evidence_refs)
        support &= any(r == 'comparison' or r.startswith(('comparison.rows.', 'historical_quality.results.'))
                       for r in statement.evidence_refs)
    uncertainty = any('No root cause is established' in s.message for s in report.uncertainty)
    uncertainty &= any('Source completeness is unverified' in s.message for s in report.missing_evidence)
    if not case['quality']:
        uncertainty &= any('Historical quality observations are missing' in s.message for s in report.missing_evidence)
    checks = dict(diagnosis_accuracy=report.evidence.status == expected['status'] and
                  (row.finding if row else None) == expected['finding'] and
                  set(selected) <= set(expected['allowed_causes']), numerical_correctness=numbers,
                  evidence_support=bool(support), appropriate_uncertainty=bool(uncertainty),
                  tool_usage=report.workflow_steps == ['collect','retrieve','assess','validate'] and
                  report.model_calls == (1 if live else 0) and
                  (service_calls is None or service_calls == ['snapshot','catalogue','compare','freshness','quality']))
    checks['payload_validation'] = audit_rows(case)[1] == expected['invalid_reasons']
    checks['quality_evidence'] = [q.model_dump(mode='json') for q in report.evidence.historical_quality.results] == \
        [q.model_dump(mode='json') for q in quality_page(case).results]
    if live:
        checks['model_completion'] = report.model_status == 'completed'
    cost = None
    if usage is not None and rates is not None:
        cost = str((Decimal(usage['input_tokens']) * rates[0] +
                    Decimal(usage['output_tokens']) * rates[1]) / Decimal(1_000_000))
    targets = expected['candidate_targets']
    return dict(case_id=case['id'], checks=checks, passed=all(checks.values()),
        model_status=report.model_status, selected_causes=selected,
        candidate_recall=(len(set(targets) & set(selected)) / len(targets) if live and targets else None),
        latency_ms=round(elapsed_ms, 3), model_calls=report.model_calls,
        usage=usage, estimated_cost_usd=cost,
        cost_status='estimated' if cost is not None else 'not_applicable' if not live else 'unknown',
        report=report.model_dump(mode='json'))


def run_benchmark(live=False, service_factory=OfflineService):
    config = ModelConfig.from_env()
    if live and not config.configured:
        return dict(schema_version=1, mode='live_model', status='blocked',
                    reason='OPENAI_API_KEY and DATALENS_OPENAI_MODEL are required', cases=[])
    rates = None
    if live and os.getenv('DATALENS_INPUT_USD_PER_MILLION') and os.getenv('DATALENS_OUTPUT_USD_PER_MILLION'):
        rates = tuple(Decimal(os.environ[k]) for k in
                      ('DATALENS_INPUT_USD_PER_MILLION','DATALENS_OUTPUT_USD_PER_MILLION'))
        if any(not r.is_finite() or r < 0 for r in rates):
            raise ValueError('Prices must be finite and nonnegative')
    results = []
    for index, case in enumerate(load_cases()):
        service = service_factory(case)
        model = OpenAIModel(config) if live else NoModel()
        started = perf_counter()
        with patch('datalens.investigation.uuid4', return_value=UUID(int=index+1)):
            report = InvestigationWorkflow(service, model).run(AssistantRequest(**case['request']))
        results.append(evaluate(case, report, getattr(service, 'calls', None),
                               (perf_counter()-started)*1000, model.usage, rates, live))
    completed = all(r['model_status'] == 'completed' for r in results) if live else True
    return dict(schema_version=1, mode='live_model' if live else 'deterministic',
        corpus_sha256=sha256(CORPUS.read_bytes()).hexdigest(), model=config.model if live else None,
        status='passed' if completed and all(r['passed'] for r in results) else 'failed',
        summary=dict(cases=len(results), passed=sum(r['passed'] for r in results),
            check_pass_counts={key:sum(r['checks'][key] for r in results) for key in results[0]['checks']},
            mean_latency_ms=round(sum(r['latency_ms'] for r in results)/len(results),3),
            min_latency_ms=min(r['latency_ms'] for r in results),
            max_latency_ms=max(r['latency_ms'] for r in results),
            model_calls=sum(r['model_calls'] for r in results),
            estimated_cost_usd=str(sum(Decimal(r['estimated_cost_usd']) for r in results))
                if all(r['estimated_cost_usd'] is not None for r in results) else None),
        limitations=['Latency is single-run wall time, not a service SLA.',
            'Candidate recall measures unconfirmed mechanism selection, not proof of root cause.',
            'Invalid payloads and genuine activity decline cannot be causally diagnosed by the current single-window assistant.',
            'Offline fixtures do not execute PostgreSQL, Kafka or Flink.',
            'Cost uses supplied uncached token rates; billing and cached-token discounts may differ.'], cases=results)
