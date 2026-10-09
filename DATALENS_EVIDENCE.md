# DataLens evidence foundation

This first phase adds a metric catalogue, declared dependencies, payment-specific freshness,
persisted operational quality observations, and a read-only revenue comparison. No LLM,
API, automatic remediation, order/payment lifecycle joins, or new service is required.

## Install

Use the existing Python environment and operations dependencies. Fresh PostgreSQL volumes
created with docker-compose.yml run init.sql followed by migration 004 automatically.
For an existing database, start PostgreSQL and apply the repeatable migration:

~~~powershell
docker compose up -d postgres
Get-Content -Raw sql/migrations/004_datalens_evidence.sql |
  docker exec -i postgres psql -v ON_ERROR_STOP=1 -U grabuser -d grabevents
~~~

This adds the datalens schema, catalogue seed records and an audit index without replacing
pipeline records. Earlier database migrations 001–003 remain prerequisites. Running it
again preserves existing catalogue edits. The index creation may briefly block writers
on a large database; apply during a maintenance interval in that case.

Enable history in the process that runs the metrics collector, then restart that collector:

~~~powershell
$env:DATALENS_EVIDENCE_ENABLED = '1'
python scripts/metrics_server.py
~~~

For the host pipeline's embedded collector, set the same environment variable before
starting the pipeline instead. Run only one collector for the same sources. For the
optional container runtime:

~~~powershell
$env:DATALENS_EVIDENCE_ENABLED = '1'
docker compose -f docker-compose.yml -f docker-compose.runtime.yml up -d --build application-metrics
~~~

Persistence is disabled by default. New payment Prometheus gauges work without migration
004; history requires that migration. Evidence failure has its own success gauge and
warning log, and never resets the Kafka observer or fails the Flink job.

## Inspect

The CLI uses POSTGRES_HOST, POSTGRES_PORT, POSTGRES_DB, POSTGRES_USER and POSTGRES_PASS,
with the same local defaults as the existing operational scripts. Credentials are not
printed. Use a SELECT-only database account for diagnostic commands when deploying.

~~~powershell
python scripts/datalens.py catalogue
python scripts/datalens.py quality --limit 20
python scripts/datalens.py compare-revenue --from-utc 2026-10-08T00:00:00Z --until-utc 2026-10-09T00:00:00Z --currency MYR --window-seconds 300
~~~

Use actual completed dates and the pipeline's actual window length. Inputs must be UTC,
window-aligned, increasing, no more than seven days, and end in the past. Results are
limited to 10,000 window groups; a larger result is rejected rather than silently truncated.
Money is returned as exact decimal strings. Diagnostics use a read-only, repeatable-read
transaction and a five-second statement timeout, and never rebuild revenue tables.
The CLI also supports migrate for applying migration 004 through a writable connection.

## Evidence and interpretation

- datalens.datasets: source locations, descriptions and owners (pipeline is the initial owner).
- datalens.metrics: definitions and versioned semantics for payment revenue and activity count.
- datalens.dependencies: explicit edges reflecting actual streaming inputs and metric outputs.
  These are declared lineage, not automatic discovery. Update source locations if topic or
  table configuration changes. No dashboard dependency is claimed before a dashboard exists.
- datalens.quality_results: UUID, observation time, check name, dataset, result status,
  scoped UTC range, and JSON evidence. Historical observations survive process restart.

The catalogue documents current semantics: valid payment amounts, separate currencies,
UTC event-time windows, immutable IDs, and manual late-data correction. Defaults in
metadata are not measurements of a running job. Malaysia business-day boundaries must
be converted from Asia/Kuala_Lumpur to UTC; a local day is not a UTC day.

Every approximately ten seconds, enabled collectors persist payment freshness and source
validation-window observations. Payment evidence contains payment-only processing/event
ages, timestamps and recent audit counts. Recent food activity cannot make this check
look fresh. Freshness is an observation, not proof of completeness or an outage.

Source snapshots preserve valid/invalid delivery counts, invalid reasons, observer start,
actual observer topic/future-skew configuration and partial/empty status. They count
observed deliveries, including duplicates. The observer seeks to the latest Kafka offsets
at startup, its window resets after failures, and backlog may make an observed window
incomplete. Persisted snapshots are overlapping observations: do not sum them to obtain
unique incident counts. Disabled collection, outages and restarts leave evidence gaps;
absence of a record means unknown. No historical Kafka backfill is implemented.

Revenue comparison groups the unique valid audit by event time and currency, then fully
joins stored revenue windows in the same database snapshot. Findings distinguish match,
missing aggregate (including zero-value payments), aggregate without audit, numerical/count
mismatch, incompatible window length and no data. A discrepancy alone does not establish
late arrival or a root cause; additional operational evidence is needed. The audit cannot
prove that all source events arrived. Reused/mutated event IDs violate its assumptions.

Prometheus additions:

- pipeline_payment_audit_freshness_seconds
- pipeline_payment_event_age_seconds
- pipeline_payment_audit_recent_rows
- pipeline_evidence_collection_success{check}

The evidence table grows while collection is enabled (about 17,280 records/day for two
checks at ten-second intervals). Set a retention policy based on required incident history
before long-term operation; this phase does not automatically delete evidence.

## Verification

~~~powershell
python -m unittest discover -s tests -v
python tests/check_datalens_sql.py
~~~

The SQL test uses the postgres container, creates isolated random schemas, exercises the
exact diagnostic SQL and repeated migration, and rolls back all writes. To use a disposable
PostgreSQL container with the same database/user, pass --container NAME. CI runs this check
in the recovery job. Unit coverage includes range validation, exact money, missing/empty
windows, incompatible configurations, observer coverage and persistence failure isolation.

The phase-two HTTP investigation API is documented in [DATALENS_API.md](DATALENS_API.md).
