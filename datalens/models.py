"""Validated public contracts for deterministic DataLens investigations."""
from datetime import datetime, timezone
from decimal import Decimal
from typing import Annotated, Literal
from uuid import UUID

from pydantic import BaseModel, BeforeValidator, ConfigDict, Field, JsonValue, model_validator
from datalens.evidence import validate_range


def utc_timestamp(value):
    if isinstance(value, datetime):
        return value.replace(tzinfo=timezone.utc) if value.tzinfo is None else value.astimezone(timezone.utc)
    return value


Timestamp = Annotated[datetime, BeforeValidator(utc_timestamp)]


class Contract(BaseModel):
    model_config = ConfigDict(extra='forbid', allow_inf_nan=False)


class RevenueRequest(Contract):
    from_utc: str = Field(max_length=40)
    until_utc: str = Field(max_length=40)
    currency: str = Field(default='MYR', pattern=r'^[A-Z]{3}$')
    window_seconds: int = Field(default=300, strict=True, ge=1, le=86400)

    @model_validator(mode='after')
    def completed_aligned_range(self):
        validate_range(self.from_utc, self.until_utc, self.currency, self.window_seconds)
        return self


class InvestigationRequest(RevenueRequest):
    metric: Literal['payment_revenue'] = 'payment_revenue'
    quality_limit: int = Field(default=100, strict=True, ge=1, le=1000)


class Dataset(Contract):
    name: str
    description: str
    owner: str
    physical_location: str


class Metric(Contract):
    name: str
    definition: str
    source_dataset: str
    semantics: dict[str, JsonValue]
    definition_version: int


class Dependency(Contract):
    upstream: str
    downstream: str
    description: str


class Catalogue(Contract):
    datasets: list[Dataset]
    metrics: list[Metric]
    dependencies: list[Dependency]


class DependencyResult(Contract):
    node: str
    direction: Literal['upstream','downstream']
    edges: list[Dependency]
    semantics: str = 'Declared transitive lineage; not runtime proof of impact.'


class QualityResult(Contract):
    result_id: UUID
    observed_at: Timestamp
    check_name: str
    dataset: str
    status: Literal['observed','partial','unknown','match','mismatch']
    range_start: Timestamp
    range_end: Timestamp
    evidence: dict[str, JsonValue]


class QualityPage(Contract):
    results: list[QualityResult]
    truncated: bool
    limit: int
    limitations: str = 'Overlapping observations; do not sum snapshots. Missing records and observer coverage do not establish source completeness.'


class PaymentFreshness(Contract):
    observed_at: Timestamp
    payment_count: int
    latest_processed_at: Timestamp | None
    latest_ingested_at: Timestamp | None
    latest_event_timestamp_ms: int | None
    processing_age_seconds: Decimal | None
    event_age_seconds: Decimal | None
    recent_payment_count: int


class RevenueWindow(Contract):
    window_start: Timestamp
    window_end: Timestamp | None
    audit_count: int | None
    audit_revenue: Decimal | None
    stored_count: int | None
    stored_revenue: Decimal | None
    finding: Literal['match','mismatch','missing_aggregate','aggregate_without_audit','incompatible_window_configuration']
    revenue_delta: Decimal


class RevenueComparison(Contract):
    status: Literal['match','mismatch','no_data']
    observed_at: Timestamp
    from_utc: Timestamp
    until_utc: Timestamp
    currency: str
    window_seconds: int
    rows: list[RevenueWindow]
    limitations: str


class Finding(Contract):
    code: str
    severity: Literal['info','warning']
    message: str
    evidence_refs: list[str]


class Investigation(Contract):
    investigation_id: UUID
    observed_at: Timestamp
    status: Literal['discrepancy','no_discrepancy_found','insufficient_evidence']
    request: InvestigationRequest
    metric_definition: Metric
    dependencies: DependencyResult
    comparison: RevenueComparison
    current_payment_freshness: PaymentFreshness
    historical_quality: QualityPage
    findings: list[Finding]
    uncertainty: list[str]
    recommended_actions: list[str]
    consistency: str = 'One read-only PostgreSQL REPEATABLE READ snapshot for all evidence.'
