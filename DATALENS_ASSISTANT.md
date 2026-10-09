# DataLens phase three

Phase three adds a fixed LangGraph investigation and hosted OpenAI assessment to the
existing read-only evidence API. Open http://127.0.0.1:8010/ for the investigation interface
and http://localhost:3000 for Grafana. Grafana's operations dashboard links to DataLens;
the interface links back to Grafana. Both links assume the documented local ports.

## Setup

Apply migration 004 and configure evidence history as described in
[DATALENS_EVIDENCE.md](DATALENS_EVIDENCE.md). Phase three requires no new database tables.
Use Python 3.11 and install the standalone API requirements:

~~~powershell
.\.venv\Scripts\python.exe -m pip install -r requirements-datalens-api.txt
~~~

Set OPENAI_API_KEY privately in the process environment using your own OpenAI API key.
Do not paste it into source code, this document, the browser, or chat. Then set:

~~~powershell
$env:DATALENS_OPENAI_MODEL = 'gpt-4.1-mini'
.\.venv\Scripts\python.exe scripts/datalens_api.py
~~~

The model name above is an example. Choose a Responses API model available to your
OpenAI project that supports structured outputs. There is no implicit model default.
If either the key or model is missing, the service remains usable in evidence mode and
returns model_status=unconfigured without a hosted request. The configuration endpoint
only reports presence and the configured model, never credentials. The application does
not automatically read .env files. Compose can use your exported variables or its own
local .env; local environment files are excluded from Git and Docker build contexts.

For Docker:

~~~powershell
docker compose -f docker-compose.yml -f docker-compose.datalens.yml up -d --build datalens-api
~~~

The overlay forwards OPENAI_API_KEY, DATALENS_OPENAI_MODEL, DATALENS_API_KEY,
DATALENS_LLM_TIMEOUT_SECONDS and DATALENS_LLM_MAX_OUTPUT_TOKENS. Keep the API's existing
loopback publishing. Runbook files are copied into the image; rebuild when they change.
No producer, Flink job, collector or Grafana restart is needed for host API operation.

| Variable | Default | Bound / purpose |
| --- | --- | --- |
| OPENAI_API_KEY | unset | Server-side OpenAI credential |
| DATALENS_OPENAI_MODEL | unset | Explicit structured-output Responses model |
| DATALENS_LLM_TIMEOUT_SECONDS | 30 | HTTP timeout per phase, clamped to 1–60 seconds |
| DATALENS_LLM_MAX_OUTPUT_TOKENS | 1000 | Clamped to 256–2000 tokens |
| DATALENS_API_KEY | unset | Existing X-DataLens-Key protection for every /v1 route |
| DATALENS_POSTGRES_USER / DATALENS_POSTGRES_PASS | phase-two defaults | Use a dedicated SELECT-only login |

A key-enabled interface accepts only the DataLens access key. It holds that key in memory
for the current page, without local/session storage. Never enter an OpenAI key there.

## Restricted database access

The workflow calls EvidenceService.investigate once. All database-backed sections remain
in one read-only REPEATABLE READ snapshot with five-second statement timeouts. The snapshot
closes before retrieval or model calls. Models receive no connection, credentials, SQL
interface, shell, Kafka client, Flink controls, or repair tools. Phase-one writers retain
their separate credentials. No automatic role provisioning or migration happens at startup.

For deployment, create a separate login as a database administrator, using a private
password prompt (for example psql's \password), and grant only:

~~~sql
CREATE ROLE datalens_api LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE;
GRANT CONNECT ON DATABASE grabevents TO datalens_api;
GRANT USAGE ON SCHEMA public, datalens TO datalens_api;
GRANT SELECT ON public.processed_events, public.payment_revenue_windows,
  datalens.datasets, datalens.metrics, datalens.dependencies, datalens.quality_results
  TO datalens_api;
ALTER ROLE datalens_api SET default_transaction_read_only = on;
~~~

Set DATALENS_POSTGRES_USER=datalens_api and DATALENS_POSTGRES_PASS privately. Do not grant
writer-role membership or sequence access. Verify effective privileges in your environment;
existing public/role grants can confer privileges independently. The API's original local
credentials remain compatible, but a restricted login is the documented deployment setup.

## Requests and evidence

| Method and path | Result |
| --- | --- |
| GET /v1/assistant/config | Secret-free hosted model configuration and call/concurrency limits |
| GET /v1/runbooks?q=late%20payment%20window&limit=4 | Bounded runbook excerpts with file version and line citations |
| POST /v1/assistant/investigations | Cited observations, suspected causes, missing evidence, uncertainty and manual next steps |

The assistant request extends the phase-two investigation body with question (1–1000
characters). It retains validated UTC, currency, completed/aligned window and quality
limits. Unsupported fields, SQL requests as fields and unsupported metrics are rejected.
For example, send this body to /v1/assistant/investigations:

~~~json
{
  "metric": "payment_revenue",
  "from_utc": "2026-10-08T00:00:00Z",
  "until_utc": "2026-10-08T01:00:00Z",
  "currency": "MYR",
  "window_seconds": 300,
  "quality_limit": 100,
  "question": "What evidence explains the stored revenue disagreement?"
}
~~~

Use dates containing your own data and the running job's actual window length. The
interface deliberately asks for explicit UTC values. It does not reinterpret Malaysia
business-day boundaries. Save report JSON from the interface for incident records;
reports are not stored server-side.

Each answer includes the complete phase-two evidence report plus:

- observations: the deterministic findings, with resolvable citations;
- suspected_causes: explicitly unconfirmed mechanisms selected by the model;
- missing_evidence and uncertainty: coverage gaps and limits, even when the model succeeds;
- manual_next_steps: evidence-service recommendations and runbook review;
- citations: exact selected evidence or runbook excerpts;
- model_status, model_calls and workflow_steps: assessment availability and bounded execution.

Metadata retrieval reuses the metric definition/version and declared upstream lineage
from the same evidence snapshot. Runbook retrieval is local lexical search over a fixed
allowlist: DATALENS_EVIDENCE.md, EVENT_TIME_BUSINESS_LOGIC.md,
CORRECTNESS_AND_RECOVERY.md and OPERATIONAL_DASHBOARDS_AND_BENCHMARKS.md. It does not crawl
arbitrary paths, URLs or environment files. Excerpts include content hashes and source
line ranges, making the cited version reviewable. Missing, oversized or symlinked files
are reported. Metadata/runbooks describe intended behavior; they are not runtime proof.

## Bounds and hosted-data policy

The graph runs collect → retrieve → assess → validate → END, with recursion_limit=6.
There are no loops, dynamic model tools, retries, checkpointers or persistent conversation
memory. At most four investigations run concurrently per process; excess requests return
429 with Retry-After. Each investigation makes at most one OpenAI call, with retries disabled.
The HTTP timeout applies to network phases rather than an overall wall-clock SLA. Existing
PostgreSQL connection and statement timeouts remain enforced independently.

Hosted input is capped at 48,000 UTF-8 bytes. It includes scope, metric definition, declared
lineage, finding counts over every comparison row, at most 12 detail windows, current
freshness, quality coverage and at most eight quality observation summaries, plus at most
four runbook excerpts. Omitted comparison details are counted explicitly; the full evidence
report remains returned locally. Free-form quality payloads and raw event/user identifiers
are excluded from model input. Runbook search allows at most six excerpts per standalone
request, 100 KB per allowlisted file, 200 chunks per file and 2400 characters per excerpt.
Review catalogue and runbook content before hosting sensitive operational metadata.

The OpenAI adapter uses the official API URL explicitly, disables ambient HTTP proxies,
sets store=false and does not supply tools. LangSmith tracing is explicitly disabled for
the graph even if ambient tracing is enabled. Application logs do not include model prompts,
SDK error bodies, keys or database details. OpenAI still receives the submitted scoped
content; store=false does not change your account's data retention policy.

Model output only selects a small schema of candidate mechanisms and references. It cannot
supply observations, repair commands or arbitrary explanatory text. Validation requires
retrieved observation AND runbook citations, rejects invented/duplicate references and
causes inferred solely from audit agreement or no data, and retains deterministic wording
that requires further evidence. Citation validation establishes provenance, not causality.
The possible mechanisms are late arrival, window configuration, source delivery, and sink
or recovery issues. This intentionally conservative assistant does not diagnose arbitrary
metrics or establish a business revenue decline against a previous day.

Missing configuration, network/auth/rate-limit failures, refusals, incomplete responses,
invalid citations and input budget exhaustion return the evidence report with an explicit
model status and no accepted suspected causes. Database failures still return sanitized 503.
Repair SQL, replay and restart remain separate operator actions following reviewed runbooks.

## Verification

~~~powershell
.\.venv\Scripts\python.exe -m unittest discover -s tests -v
.\.venv\Scripts\python.exe -m pip check
node --check datalens/static/app.js
docker compose -f docker-compose.yml -f docker-compose.datalens.yml config --quiet
.\.venv\Scripts\python.exe tests/check_datalens_api.py
~~~

Unit tests use fake models without credentials or external requests. They exercise the
real LangGraph, provenance validation, unsupported requests, prompt injection, missing
runbooks, provider failures/refusal, input/call/concurrency budgets, secret-free config,
authentication and unchanged snapshot semantics. The PostgreSQL integration check extends
the existing disposable-database check through the assistant HTTP endpoint using a fake
model and verifies business/evidence rows remain unchanged. CI runs it with PostgreSQL.
A live OpenAI smoke test requires a privately configured key, an accessible model and real
evidence; it is intentionally not part of the offline test suite.

Implementation references: [OpenAI structured outputs](https://developers.openai.com/api/docs/guides/structured-outputs?api-mode=responses)
and [LangGraph graph API](https://docs.langchain.com/oss/python/langgraph/graph-api).

Verification performed on 2026-10-09: all 74 unit tests passed (18 new phase-three tests),
Python compilation, real-SDK offline serialization/parsing, JavaScript syntax, dependency
consistency, Compose validation and Git whitespace checks passed. Live PostgreSQL integration
was unavailable because Docker was not running. Live OpenAI validation was unavailable
because no key/model configuration was present. Browser visual/interaction verification
was unavailable because the UI tool had no browser. UI assets and API behavior were checked
through TestClient; no production database or hosted-model calls were used for these checks.
