"""Phase-three contracts. Model output can select hypotheses, never create observations."""
from typing import Literal
from pydantic import Field
from datalens.models import Contract, Investigation, InvestigationRequest

Cause = Literal['late_arrival', 'window_configuration', 'source_delivery', 'sink_or_recovery']


class AssistantRequest(InvestigationRequest):
    question: str = Field(default='Investigate payment revenue discrepancies and evidence gaps.',
                          min_length=1, max_length=1000)


class HypothesisSelection(Contract):
    cause: Cause
    evidence_refs: list[str] = Field(min_length=1, max_length=6)


class ModelAssessment(Contract):
    hypotheses: list[HypothesisSelection] = Field(max_length=4)


class CitedStatement(Contract):
    message: str
    evidence_refs: list[str] = Field(min_length=1)


class Citation(Contract):
    ref: str
    kind: Literal['evidence', 'metadata', 'runbook']
    title: str
    content: str
    source: str | None = None
    version: str | None = None
    line_start: int | None = None
    line_end: int | None = None


class RetrievalPage(Contract):
    results: list[Citation]
    missing_documents: list[str]
    semantics: str


class AssistantReport(Contract):
    evidence: Investigation
    observations: list[CitedStatement]
    suspected_causes: list[CitedStatement]
    missing_evidence: list[CitedStatement]
    uncertainty: list[CitedStatement]
    manual_next_steps: list[CitedStatement]
    citations: list[Citation]
    model_status: Literal['completed', 'unconfigured', 'unavailable', 'invalid_output', 'budget_exceeded']
    model: str | None
    workflow_steps: list[str]
    model_calls: int
    policy: str = 'Read-only evidence. Suspected causes are unconfirmed. Repairs require manual review.'
