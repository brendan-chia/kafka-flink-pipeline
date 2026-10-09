# DataLens investigation API — phase two

The standalone FastAPI service exposes phase-one diagnostics as typed HTTP responses.
It performs deterministic investigations for payment_revenue, with no LLM, arbitrary SQL,
repair actions, database migration endpoint or automatic background collection.
The existing Kafka/Flink processing path does not depend on this service.

## Start locally

Apply migration 004 using [DATALENS_EVIDENCE.md](DATALENS_EVIDENCE.md), then:

~~~powershell
.\.venv\Scripts\python.exe -m pip install -r requirements-datalens-api.txt
.\.venv\Scripts\python.exe scripts/datalens_api.py
~~~

The API listens on http://127.0.0.1:8010. Open /docs for interactive requests or
/openapi.json for the machine-readable schemas. The metrics collector keeps port 8000.
Use --host and --port to change the API bind address. No pipeline restart is needed.
Enable phase-one history collection separately if historical quality evidence is wanted.

The API uses POSTGRES_HOST, POSTGRES_PORT and POSTGRES_DB, with the existing local
POSTGRES_USER/POSTGRES_PASS defaults. DATALENS_POSTGRES_USER and DATALENS_POSTGRES_PASS
override only the API account. A dedicated account with SELECT access to public.processed_events,
public.payment_revenue_windows and all four datalens tables can serve every endpoint.
Connections use read-only REPEATABLE READ transactions and five-second statement timeouts.
There is no SQL input endpoint. Phase-one writers need their own writable credentials.

Optional DATALENS_API_KEY requires X-DataLens-Key on every /v1 endpoint, including readiness.
Liveness and the documentation/schema endpoints remain accessible. The key is read from the
environment and is never returned. Default host/container publishing is restricted to loopback.

## Run with Docker

~~~powershell
docker compose -f docker-compose.yml -f docker-compose.datalens.yml up -d --build datalens-api
~~~

The optional overlay works independently of docker-compose.runtime.yml. The API image
uses its own dependencies and a non-root user. Port 8010 is published only on 127.0.0.1.
Fresh database volumes receive migration 004 from the base Compose file; existing volumes
still need that migration applied explicitly. Container health checks indicate liveness;
/v1/health/ready checks database access and required tables. This service does not start
or restart the producer, metrics collector, or Flink job.

## Endpoints

| Method and path | Result |
| --- | --- |
| GET /health/live | Process liveness; no database query |
| GET /v1/health/ready | Required database objects are queryable |
| GET /v1/catalogue | Dataset descriptions, metric definitions and declared dependencies |
| GET /v1/dependencies?node=metric:payment_revenue&direction=upstream | Transitive declared lineage, with cycle protection |
| GET /v1/quality?from_utc=...&until_utc=... | Overlapping historical observations; optional dataset/check_name filters and limit |
| GET /v1/payments/freshness | Current payment-only audit freshness |
| POST /v1/revenue/compare | Read-only completed-window comparison |
| POST /v1/investigations | Definition, lineage, comparison, current freshness, historical quality and findings |

Quality ranges use explicit UTC timestamps with whole-second precision and need not
align to aggregation windows. Every range must increase, end in the past and cover at
most seven days. Window comparisons additionally require alignment to window_seconds.
Quality limits are 1–1000; a truncated flag explicitly identifies a limited sample.
The query includes overlapping intervals and instantaneous observations within [start,end).

## Investigation request

~~~powershell
$body = @{
    metric = 'payment_revenue'
    from_utc = '2026-01-01T00:00:00Z'
    until_utc = '2026-01-01T00:05:00Z'
    currency = 'MYR'
    window_seconds = 300
    quality_limit = 100
} | ConvertTo-Json
Invoke-RestMethod -Method Post -Uri http://127.0.0.1:8010/v1/investigations -ContentType application/json -Body $body
~~~

Use dates containing your own data and the actual pipeline window length. If an API key
is enabled, add -Headers @{ 'X-DataLens-Key' = $env:DATALENS_API_KEY } to requests.
For Malaysia daily analytics, convert the local day boundaries to UTC before sending.
Unknown fields, unsupported metrics, malformed currencies, ambiguous timestamps, boolean
window lengths and unbounded ranges are rejected with HTTP 422 before diagnostic queries.

## Response interpretation

The response contains an investigation UUID, observation timestamp, request scope,
metric definition/version, upstream lineage, comparison rows, current freshness, scoped
quality history, evidence-linked findings, uncertainty and suggested manual next actions.
IDs identify returned reports; this phase does not persist investigation reports or expose
an investigation-history endpoint. Save the returned JSON if a report must be retained.

Investigation status is one of:

- discrepancy: at least one stored group differs from the current valid audit or has an
  incompatible window length. This is evidence of disagreement, not an established cause.
- no_discrepancy_found: all compared groups agree with the audit. This does not establish
  complete source delivery or trustworthy business performance.
- insufficient_evidence: neither audit nor stored groups exist in the requested scope.

All database-backed sections of an investigation are read in one consistent PostgreSQL
snapshot. Decimal values serialize as strings, preserving exact money. Database timestamps
without timezone are explicitly treated as UTC, following the existing pipeline contract.
Historical quality is scoped to the payment audit/source/aggregate datasets. Overlapping
snapshots are not additive counts, and the sample does not establish continuous coverage.
Current freshness is measured at investigation time, not reconstructed for the incident.
Missing history remains unknown. Declared dependencies do not prove runtime impact.

The API does not infer that a payment job failed or that late payments caused the discrepancy.
Those conclusions need additional evidence and the later AI workflow. It also does not
calculate a revenue decline against a previous day in this phase.

Unavailable databases, query timeouts and missing schema return HTTP 503 with a sanitized
message; credentials, connection details and SQL are not returned. Unknown dependency nodes
return 404. This API never performs repairs, even when the response suggests manual inspection.

## Verification

~~~powershell
.\.venv\Scripts\python.exe -m pip install -r requirements-dev.txt
.\.venv\Scripts\python.exe -m unittest discover -s tests -v
.\.venv\Scripts\python.exe tests/check_datalens_api.py
~~~

The integration test creates a uniquely named disposable database on the configured PostgreSQL
server, verifies real HTTP-layer diagnostics and enforced read-only access, checks that API
requests leave both business/evidence rows unchanged, then removes only its test database.
It requires database-creation permission for the test account. To resolve a disposable
container's published host port, pass --container NAME. No test fixtures are added to your
project database. CI runs the API unit tests, Compose validation and real PostgreSQL check.

Phase three adds bounded OpenAI-assisted investigations and an interface alongside Grafana. See [DATALENS_ASSISTANT.md](DATALENS_ASSISTANT.md) for configuration, citations, limits and verification.
