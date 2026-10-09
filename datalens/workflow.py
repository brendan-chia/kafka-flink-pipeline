"""Fixed four-node LangGraph investigation, with no model tools or repair capability."""
from collections import Counter
import json
from threading import BoundedSemaphore
from typing import TypedDict

from langgraph.graph import StateGraph, START, END
from langsmith import tracing_context
from pydantic import ValidationError

from datalens.assistant_models import AssistantReport, ModelAssessment
from datalens.llm import OpenAIModel, ModelUnavailable, InvalidModelOutput
from datalens.models import InvestigationRequest
from datalens.retrieval import RunbookStore

MAX_INPUT_BYTES = 48_000
SLOTS = BoundedSemaphore(4)
CAUSES = {
    'late_arrival': 'Late arrival after window closure is a possible explanation; arrival and watermark evidence are needed to confirm it.',
    'window_configuration': 'Different window settings are a possible explanation; the actual running job configuration is needed to confirm it.',
    'source_delivery': 'Missing or rejected source deliveries are a possible explanation; a source census and scoped validation evidence are needed to confirm it.',
    'sink_or_recovery': 'Sink lag or recovery replay is a possible explanation; scoped job, checkpoint and sink logs are needed to confirm it.',
}


class BusyInvestigation(Exception):
    pass


class State(TypedDict, total=False):
    request: object
    evidence: object
    citations: list
    missing_documents: list
    payload: str
    assessment: object
    model_status: str
    model_calls: int
    steps: list
    result: object


def statement(message, refs):
    return dict(message=message, evidence_refs=refs)


class InvestigationWorkflow:
    def __init__(self, service, model=None, store=None):
        self.service = service
        self.model = model or OpenAIModel()
        self.store = store or RunbookStore()
        builder = StateGraph(State)
        for name in ('collect', 'retrieve', 'assess', 'validate'):
            builder.add_node(name, getattr(self, name))
        builder.add_edge(START, 'collect')
        builder.add_edge('collect', 'retrieve')
        builder.add_edge('retrieve', 'assess')
        builder.add_edge('assess', 'validate')
        builder.add_edge('validate', END)
        self.graph = builder.compile()  # No checkpointer, memory, agent loop, or tools.

    def run(self, request):
        if not SLOTS.acquire(blocking=False):
            raise BusyInvestigation()
        try:
            # Explicitly prevent ambient LangSmith configuration from exporting evidence.
            with tracing_context(enabled=False):
                return self.graph.invoke(dict(request=request, steps=[], model_calls=0),
                                         config={'recursion_limit': 6})['result']
        finally:
            SLOTS.release()

    def collect(self, state):
        request = InvestigationRequest.model_validate(state['request'].model_dump(exclude={'question'}))
        report = self.service.investigate(request)
        return dict(evidence=report, steps=['collect'])

    def retrieve(self, state):
        report = state['evidence']
        citations = []

        def cite(ref, kind, title, value):
            citations.append(dict(ref=ref, kind=kind, title=title,
                                  content=json.dumps(value, ensure_ascii=False, separators=(',', ':'))))

        data = report.model_dump(mode='json')
        cite('scope', 'evidence', 'Validated investigation scope', data['request'])
        cite('metric_definition', 'metadata', 'Versioned metric definition', data['metric_definition'])
        cite('dependencies', 'metadata', 'Declared upstream lineage', data['dependencies'])
        counts = Counter(row.finding for row in report.comparison.rows)
        # Aggregate over ALL rows; bounded detail sample never silently represents the whole range.
        sample_indexes = [i for i, row in enumerate(report.comparison.rows) if row.finding != 'match'][:12]
        if not sample_indexes:
            sample_indexes = list(range(min(12, len(report.comparison.rows))))
        cite('comparison', 'evidence', 'Complete comparison finding counts',
             dict(status=report.comparison.status, total_rows=len(report.comparison.rows),
                  finding_counts=dict(counts), detail_rows_in_prompt=len(sample_indexes),
                  details_omitted=len(report.comparison.rows)-len(sample_indexes),
                  limitations=report.comparison.limitations))
        for i in sample_indexes:
            cite(f'comparison.rows.{i}', 'evidence', f'Compared window {i}', data['comparison']['rows'][i])
        cite('current_payment_freshness', 'evidence', 'Current freshness, not incident-time freshness',
             data['current_payment_freshness'])
        # No free-form quality payloads or event/user identifiers are sent to the hosted model.
        cite('historical_quality', 'evidence', 'Scoped quality coverage',
             dict(returned=len(report.historical_quality.results), truncated=report.historical_quality.truncated,
                  status_counts=dict(Counter(r.status for r in report.historical_quality.results)),
                  limitations=report.historical_quality.limitations))
        for i, row in enumerate(report.historical_quality.results[:8]):
            safe = {key: row.evidence[key] for key in ('valid_deliveries', 'invalid_deliveries', 'coverage_start')
                    if key in row.evidence and isinstance(row.evidence[key], (int, float, str, type(None)))}
            cite(f'historical_quality.results.{i}', 'evidence', f'Quality observation {row.result_id}',
                 dict(result_id=str(row.result_id), observed_at=row.observed_at.isoformat(),
                      range_start=row.range_start.isoformat(), range_end=row.range_end.isoformat(),
                      check_name=row.check_name, dataset=row.dataset, status=row.status, counters=safe))
        retrieved = self.store.search(state['request'].question + ' payment revenue window late audit recovery evidence')
        citations.extend(retrieved['results'])
        payload = json.dumps(dict(question=state['request'].question, status=report.status,
                                  uncertainty=report.uncertainty, citations=citations,
                                  permitted_causes=CAUSES), ensure_ascii=False)
        return dict(citations=citations, missing_documents=retrieved['missing_documents'], payload=payload,
                    steps=state['steps']+['retrieve'])

    def assess(self, state):
        status, assessment, calls = 'unconfigured', None, 0
        if len(state['payload'].encode('utf-8')) > MAX_INPUT_BYTES:
            status = 'budget_exceeded'
        elif self.model.config.configured:
            calls = 1
            try:
                assessment = ModelAssessment.model_validate(self.model.assess(state['payload']))
                status = 'completed'
            except (InvalidModelOutput, ValidationError):
                status = 'invalid_output'
            except ModelUnavailable:
                status = 'unavailable'
        return dict(assessment=assessment, model_status=status, model_calls=calls,
                    steps=state['steps']+['assess'])

    def validate(self, state):
        report = state['evidence']
        citations = list(state['citations'])
        known = {c['ref']: c for c in citations}
        retrieved_refs = set(known)
        observations = []
        for finding in report.findings:
            # Findings may refer to details outside the model sample. Keep those exact citations.
            refs = finding.evidence_refs[:6]
            if len(finding.evidence_refs) > 6:
                refs = ['comparison'] + refs[:5]
            for ref in refs:
                if ref not in known and ref.startswith('comparison.rows.'):
                    index = int(ref.rsplit('.', 1)[1])
                    row = report.comparison.rows[index]
                    citation = dict(ref=ref, kind='evidence', title=f'Compared window {index}',
                                    content=row.model_dump_json())
                    citations.append(citation)
                    known[ref] = citation
            # One group can contain 10,000 windows. The complete comparison is always citable.
            observations.append(statement(finding.message, refs))
        suspected, status, seen = [], state['model_status'], set()
        if state.get('assessment') is not None:
            for hypothesis in state['assessment'].hypotheses:
                refs = hypothesis.evidence_refs
                valid = len(refs) == len(set(refs)) and all(ref in retrieved_refs for ref in refs)
                observed = any(ref == 'comparison' or ref.startswith('comparison.rows.') or
                               ref.startswith('historical_quality.results.') for ref in refs)
                runbook = any(ref in known and known[ref]['kind'] == 'runbook' for ref in refs)
                # No candidate cause is justified by empty data or audit agreement alone.
                compatible = report.status == 'discrepancy'
                if not (valid and observed and runbook and compatible and hypothesis.cause not in seen):
                    status = 'invalid_output'
                    suspected = []
                    break
                seen.add(hypothesis.cause)
                suspected.append(statement(CAUSES[hypothesis.cause], refs))
        missing = [statement('Source completeness is unverified; no source census was retrieved.', ['comparison']),
                   statement('Incident-time job, watermark, checkpoint and sink logs were not retrieved.', ['scope'])]
        if not report.historical_quality.results:
            missing.append(statement('Historical quality observations are missing for this range.', ['historical_quality']))
        if report.historical_quality.truncated:
            missing.append(statement('Quality history is a limited sample; continuous coverage is unknown.', ['historical_quality']))
        if state['missing_documents']:
            missing.append(statement('Runbook documents unavailable: ' + ', '.join(state['missing_documents']), ['scope']))
        if not any(c['kind'] == 'runbook' for c in citations):
            missing.append(statement('No relevant runbook excerpts were retrieved.', ['scope']))
        uncertainty = [statement(text, ['comparison', 'historical_quality', 'current_payment_freshness'])
                       for text in report.uncertainty]
        uncertainty.append(statement('No root cause is established; suspected mechanisms require independent confirmation.', ['comparison']))
        if status != 'completed':
            uncertainty.append(statement('Hosted model assessment was not accepted (' + status + '); deterministic evidence remains available.', ['scope']))
        actions = [statement(text, ['comparison', 'historical_quality']) for text in report.recommended_actions]
        actions.append(statement('Review the cited runbooks and obtain missing evidence before any manual repair.',
                                 [c['ref'] for c in citations if c['kind'] == 'runbook'] or ['scope']))
        steps = state['steps']+['validate']
        result = AssistantReport(evidence=report, observations=observations, suspected_causes=suspected,
            missing_evidence=missing, uncertainty=uncertainty, manual_next_steps=actions, citations=citations,
            model_status=status, model=self.model.config.model or None, workflow_steps=steps,
            model_calls=state['model_calls'])
        return dict(result=result, steps=steps)
