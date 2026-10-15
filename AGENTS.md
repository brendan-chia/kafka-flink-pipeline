# Repository guidance for coding agents

## Scope and project context

This file applies throughout the repository. Read any more specific `AGENTS.md`
before changing files in its directory. Keep changes focused on the requested task
and preserve unrelated work.

This is a local Kafka/PyFlink/PostgreSQL streaming and observability project with
two optional modules: DataLens for evidence-based investigations and JourneyPulse
for food-ordering clickstream capture and customer profiles. The active streaming
implementation is Python. `flink-processor/pom.xml` and its Java Hello World
sources are scaffolding, not the production pipeline or the main test entry point.

Use Python 3.11 and the pinned `requirements*.txt` files. The pipeline uses PyFlink
2.2.1; Java 17 is recommended and required for the optional Iceberg integration.
JourneyPulse CI uses Node.js 22 for its dependency-free ES-module SDK tests.

## Repository map

| Location | Responsibility |
| --- | --- |
| `event_contract.py`, `contracts/` | Transaction decoding, versioned JSON schemas and compatibility fixtures |
| `producer/event_producer.py` | Synthetic Kafka traffic, including opt-in faults, delays and duplicates |
| `flink-processor/pipeline.py` | Transaction validation, PostgreSQL audit sink, Kafka DLQ and event-time aggregates |
| `sql/init.sql`, `sql/migrations/` | Fresh-database schema and upgrades for existing databases |
| `monitoring/` | Application metrics, Prometheus scrapes/alerts, Alertmanager and provisioned Grafana dashboard |
| `datalens/`, `datalens/static/` | Deterministic evidence, bounded assistant workflow, FastAPI service and browser interface |
| `journeypulse/`, `contracts/journeypulse/` | Separate clickstream contracts, bootstrap, authentication, gateway, SDK, processor and customer API |
| `journeypulse/sql/` | JourneyPulse migrations and optional database capability roles |
| `lakehouse/`, `lakehouse_config.py`, `lakehouse_ops.py` | Optional Iceberg/MinIO/Trino storage, reconciliation and replay |
| `scripts/` | Service launchers, setup, evaluations, benchmarks, demos and recovery tools |
| `docker/`, `docker-compose*.yml` | Component images and optional Compose overlays |
| `tests/`, `.github/workflows/` | Python regression tests, executable integration probes and CI workflows |
| `docs/adr/`, `docs/journeypulse/` | Architecture decisions and phased JourneyPulse implementation guides |

Start with `README.md` and the guide for the component being changed. Relevant
guides include `CORRECTNESS_AND_RECOVERY.md`, `EVENT_TIME_BUSINESS_LOGIC.md`,
`OPERATIONAL_DASHBOARDS_AND_BENCHMARKS.md`, `ICEBERG_AND_REPLAY.md`, and
`DATALENS_{EVIDENCE,API,ASSISTANT,EVALUATION}.md`.

JourneyPulse phases 1–4 are implemented; phases 5–6 are planned. Consult
`journeypulse/README.md` and `docs/journeypulse/` before extending the module.
Keep implementation status and measured evidence accurate when updating guides.

## Development commands

Run commands from the repository root. On Windows, prefer the existing virtual
environment explicitly; activation is optional:

```powershell
.\.venv\Scripts\python.exe -m pip install -r requirements-dev.txt
.\.venv\Scripts\python.exe -m unittest discover -s tests -v
node --test journeypulse/sdk/journeypulse.test.mjs
```

Create `.venv` with `python -m venv .venv` if needed. On Linux/macOS use
`.venv/bin/python`, or `python` after activating the environment. Use narrower
dependency files when working on a standalone service:

- `requirements.txt`: core pipeline and producer dependencies.
- `requirements-operations.txt`: lightweight operational tooling without PyFlink.
- `requirements-datalens-api.txt`: DataLens API and assistant dependencies.
- `requirements-journeypulse.txt`: JourneyPulse foundation.
- `requirements-journeypulse-gateway.txt`: JourneyPulse HTTP services.
- `requirements-journeypulse-processor.txt`: separate JourneyPulse PyFlink runtime.

Download pipeline connectors and the Prometheus reporter with
`bash download_jars.sh` in Git Bash, WSL or Linux. For lakehouse work, also use
`python scripts/download_lakehouse_jars.py` and follow `ICEBERG_AND_REPLAY.md`.
Keep JAR versions, download helpers and Docker images compatible with PyFlink.

The base `docker compose up -d` starts supporting infrastructure. Host processes
use `python flink-processor/pipeline.py` and `python producer/event_producer.py`.
Host Kafka connections use `localhost:29092`; containers use `kafka:9092`.
Compose overlays must be combined with `docker-compose.yml`:

```powershell
docker compose -f docker-compose.yml -f docker-compose.runtime.yml --profile traffic --profile tools config --quiet
docker compose -f docker-compose.yml -f docker-compose.datalens.yml config --quiet
docker compose -f docker-compose.yml -f docker-compose.journeypulse.yml config --quiet
```

The runtime overlay supplies the transaction Flink cluster and metrics collector;
`traffic` enables its producer and `tools` enables its probe. Other overlays add
DataLens, an isolated DataLens demo, JourneyPulse, or the optional lakehouse.
Follow each module's guide for startup order and bootstrap prerequisites.

## Correctness rules

- Keep transaction schema, `event_contract.py`, producer output and compatibility
  tests aligned. Retained transaction events without `schema_version` are treated
  as v1; explicit unsupported versions are rejected. JourneyPulse has its own
  strict schema and catalogue; do not route clickstream through the transaction
  contract or silently change registered JourneyPulse v1 definitions.
- Preserve exact money with `Decimal` and SQL decimal types. Reject non-finite
  values, booleans as amounts, negative amounts and fractional cents. Keep
  currency dimensions in revenue comparisons and aggregates.
- Preserve malformed input bytes and Kafka topic/partition/offset provenance in
  DLQ or quarantine records. Invalid transaction records must never enter business
  sinks or advance watermarks with unvalidated timestamps.
- Treat event IDs as immutable business identities and Kafka record IDs as
  delivery identities. Preserve idempotent PostgreSQL writes and durable replay
  receipts. Exactly-once Flink checkpoints do not imply exactly-once delivery
  across external sinks; the transaction DLQ is at-least-once.
- Keep event time, broker ingestion time and processing time distinct. Pipeline
  windows use UTC and half-open ranges. Late valid transaction events remain in
  the audit but can miss finalized streaming windows; reconciliation includes
  them. Test watermark, idleness, deduplication and boundary behavior when changing
  window logic.
- JourneyPulse uses dedicated topics, schema, consumer group and Flink job. Keep
  it independent of transaction processing and DataLens. Its profile sink commits
  receipts, deduplication decisions and summary effects transactionally. Event-ID
  retention is seven days from processing; order identities and delivery receipts
  are durable. Late events must not move latest-activity timestamps backwards.
- JourneyPulse behavior must not overwrite onboarding attributes or explicit
  consent. Placeholders default to consent false. Preserve customer-scoped
  authorization and the distinction between tracking and customer-serving tokens.
- DataLens investigations use bounded, parameterized, read-only queries in a
  consistent snapshot. Keep arbitrary SQL and repair execution out of the API and
  assistant. Separate evidence writers and explicit operational repair tools.
  Preserve query limits, timeouts, reviewed runbook retrieval, citations and the
  distinction between observed facts and unproven causes.
- Lakehouse replay and repair must preserve snapshot IDs and provenance. Keep
  replay targets separate from live source/DLQ topics and retain the existing
  preview/explicit-execution behavior.

## Schema, configuration and implementation conventions

- Add an upgrade migration for existing databases and update fresh initialization
  where appropriate. Docker initialization scripts run only on fresh volumes.
  JourneyPulse bootstrap applies its own migrations and must remain repeatable.
  Document migration order and any required pause of writers.
- Follow the nearby Python style, type usage and module organization. There is no
  repository-wide formatter or linter configuration; do not introduce one or
  reformat unrelated files as part of a small change.
- Keep shared validation and business rules in their existing modules rather than
  duplicating them in launch scripts, HTTP handlers or test helpers. Use
  parameterized SQL for values and validate dynamic identifiers.
- Preserve environment-based configuration and optional component boundaries.
  Update relevant Dockerfiles, dependency files, Compose settings and guides when
  adding a runtime dependency or configuration setting.
- Keep metrics labels bounded; do not label metrics with user IDs, event IDs or
  raw errors. Distinguish liveness, readiness, collection health, freshness and
  consumer lag. Update alert regression fixtures when alert semantics change.
- Keep credentials in environment variables; never include real secrets in
  code, fixtures, logs or reports. Existing committed credentials are local demo
  defaults.
- Do not edit vendored environments (`kafka/`, `.venv/`), downloaded `jars/` or
  `plugins/`, generated bytecode, logs or runtime state as source. Preserve
  checkpoints, receipts and container volumes during normal development.
  Destructive resets and failure-injection drills require task scope that calls
  for them; use isolated test resources for database mutations and replay.

## Verification and handoff

Use Python `unittest` for regression tests and Node's built-in test runner for the
SDK. `tests/test_*.py` are discovery tests; `tests/check_*.py` are separate
executable probes with varying Java, connector and live-service requirements.

| Change | Relevant checks beyond focused regression tests |
| --- | --- |
| Transaction validation/routing | `python tests/check_flink_plan.py`, `python tests/check_flink_validator.py` |
| Event-time behavior | `python tests/check_event_time_windows.py`, `python tests/check_late_event_policy.py`; live PostgreSQL: `python tests/check_event_time_sql.py` |
| Operational SQL/metrics | `python tests/check_operational_sql.py --docker`; Prometheus configuration and alert tests from `.github/workflows/pipeline.yml` |
| DataLens SQL/API | `python tests/check_datalens_sql.py`, `python tests/check_datalens_api.py` against configured infrastructure |
| DataLens evaluation | `python scripts/evaluate_datalens.py --output .flink-state/evaluation-offline.json`; database/demo sequence from `.github/workflows/datalens-evaluation.yml` |
| JourneyPulse | `python -m unittest discover -s tests -p 'test_journeypulse*.py' -v`, SDK tests, `python scripts/journeypulse.py validate`, `python tests/check_journeypulse_plan.py` |
| JourneyPulse integration | Capture, database and profile probes in `.github/workflows/journeypulse.yml`; use its dedicated cluster for restart tests |
| Lakehouse | `python tests/check_lakehouse_plan.py`, `python tests/check_lakehouse_roundtrip.py` with the guide's prerequisites |
| Compose/Docker | Render the affected overlay with `config --quiet`; build or exercise affected images when available |

Run focused checks while iterating and the full Python suite for cross-cutting
changes when dependencies are available. Add regression coverage for meaningful
behavior changes, especially duplicate delivery, invalid records, rollback,
authorization, late data and recovery. Do not start live services or mutate a
database just to verify a documentation-only edit.

Use `.github/workflows/pipeline.yml`, `datalens-evaluation.yml` and
`journeypulse.yml` as the executable reference for integration prerequisites and
check order. Report what changed, which checks ran, and any checks skipped because
Java, Docker, Kafka, PostgreSQL, connector JARs or credentials were unavailable.
Never describe an offline unit test or graph-construction check as proof of live
transport, checkpoint recovery or measured latency.
