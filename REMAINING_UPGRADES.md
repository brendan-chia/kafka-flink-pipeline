# Recommendation audit and next implemented upgrade

Reviewed against the supplied recommendation on 2026-10-08. Existing working
tree improvements were inspected and preserved. A file's presence is not proof
of a live end-to-end result.

| Recommendation | Repository status after this change |
| --- | --- |
| Exhaustive validation, malformed-payload DLQ | Already implemented in `event_contract.py` and the raw-byte Flink source |
| Stable IDs and idempotent writes | Already implemented, unique PostgreSQL key and JDBC upserts |
| Exact money and timestamp provenance | Already implemented, decimal amounts, currency, event/broker/processing timestamps |
| Persistent checkpoints and restoration | Already implemented for the host runtime; shared local container volume added |
| Failure after writes, replay reconciliation | Existing manual probe; automated TaskManager crash drill and CI evidence added |
| Event-time revenue, duplicate/delay injection | Already implemented; actual bounded window test compares with a batch oracle |
| Related order/payment lifecycle, failed-payment success rate by city | Still absent; current events model independent activity and successful payments only |
| Repeated payment attempts and temporal merchant join | Still absent; need lifecycle identities and versioned merchant history |
| Watermarks, idle inputs and late corrections | Already implemented with explicit finalized-window reconciliation |
| Iceberg raw/validated history and replay | Already implemented as an optional stack, with maintenance/evolution scripts |
| Grafana, latency/freshness, runtime alerts and runbooks | Already implemented; container-specific Prometheus targets added |
| Normal/fault traffic and recent invalid fractions | Already implemented |
| Containerized producer, JobManager, TaskManager | Added optional runtime overlay and separate metrics collector |
| Versioned contract and compatibility checks | Added schema v1, explicit producer version, historical compatibility corpus and runtime tests |
| Automated integration checks | Added CI for Kafka/Flink/PostgreSQL transport, crash/replay, SQL, alert rules and artifacts |
| Capacity benchmarks and measured numbers | Runner already exists; actual capacity results still need execution on recorded hardware |
| Architecture decisions and incident evidence | Added runtime/contract ADR; drill emits measured incident/recovery JSON when executed |
| Optional OLAP / AI operations assistant | No workload or incident evaluation justifies these additions yet |

This upgrade addresses deployment and reproducible reliability evidence first.
It keeps the current normalized row shape and four business outputs. Order
lifecycle analytics are a separate contract/business-model upgrade: treating
random activity as related orders or failed payments would give misleading
success-rate and fraud metrics.

## Container runtime

The implementation follows the official [Flink Docker session-cluster setup](https://nightlies.apache.org/flink/flink-docs-release-2.2/docs/deployment/resource-providers/standalone/docker/)
and [Python CLI submission](https://nightlies.apache.org/flink/flink-docs-release-2.2/docs/deployment/cli/#submitting-pyflink-jobs).
The images pin Flink 2.2.1 with Java 17 and Python operation dependencies. The
Flink image installs Python and connectors at build time, so no host Java,
Python environment or manual JAR download is needed for the container workflow.

Stop any host producer/pipeline/metrics server before switching to this runtime.
Use the existing migration guides for databases predating migrations 001–003;
this upgrade does not require another database migration. Preserve data volumes.

```powershell
docker compose -f docker-compose.yml -f docker-compose.runtime.yml up -d --build
docker compose -f docker-compose.yml -f docker-compose.runtime.yml --profile traffic up -d producer
```

The first command starts infrastructure, JobManager, TaskManager, the submission
client and metrics. Continuous traffic requires the second command. Inspect the
Flink UI at <http://localhost:8081> and the existing Grafana dashboard. Check logs:

```powershell
docker compose -f docker-compose.yml -f docker-compose.runtime.yml logs -f pipeline taskmanager
```

The overlay mounts a separate Prometheus config with container targets. It
does not combine with the optional lakehouse overlay yet. Scaling TaskManagers
also needs scrape service discovery; the supplied targets cover one TaskManager.
The shared `flink_state` volume is local to this Docker host and survives
container replacement. It does not provide JobManager high availability.

For a normal non-destructive probe without installing host Python dependencies:

```powershell
docker compose -f docker-compose.yml -f docker-compose.runtime.yml run --rm probe python scripts/recovery_probe.py --publish --manifest .flink-state/container-probe.json
docker compose -f docker-compose.yml -f docker-compose.runtime.yml run --rm probe python scripts/recovery_probe.py --verify --manifest .flink-state/container-probe.json --timeout 180
```

The submission client is deliberately not auto-restarted. After JobManager
process loss, stop the old submission client and inspect the cluster to make
sure there is no active job before resubmitting. For a compatible retained
checkpoint, use the container URI of its `_metadata` file, for example
`file:///opt/flink/state/checkpoints/<job-id>/chk-<n>/_metadata`, as
`FLINK_RESTORE_PATH`. Then recreate the submission client with the same graph,
parallelism and event-time settings. Do not use a Windows host checkpoint path
inside a Linux container. Cross-runtime checkpoint portability is unverified.

## Automated crash and replay drill

Use a dedicated local run with the continuous producer stopped. Submit the job
with a ten-minute checkpoint interval so the drill can establish a checkpoint,
observe external writes, and stop the TaskManager before another checkpoint.
Changing the interval of an already submitted job requires resubmission; setting
the variable only in the drill terminal does not change the running job.

For a fresh dedicated environment, before the startup command:

```powershell
$env:FLINK_CHECKPOINT_INTERVAL = '10 min'
docker compose -f docker-compose.yml -f docker-compose.runtime.yml up -d --build
python -m pip install -r requirements-operations.txt
python scripts/recovery_drill.py --inject-taskmanager-failure --output .flink-state/drill-1.json
```

The script verifies exactly one active job, manually triggers a completed
checkpoint, publishes new IDs and invalid raw fixtures, reconciles external
writes, and refuses to proceed if a newer checkpoint has begun. It explicitly
sends SIGKILL to the local TaskManager service and always attempts to start it
again. It verifies Flink restored the baseline checkpoint, unique correct
PostgreSQL rows, and at least two DLQ deliveries for each invalid source record.
Repeated DLQ deliveries demonstrate the probe was replayed. The report records
recovery duration, counts, source identities, checkpoint ID and any failure.
Use a new output filename for each run.

This is a TaskManager failure test. It does not establish JobManager HA,
exactly-once DLQ semantics or lakehouse recovery. The automation uses the
[documented checkpoint REST handlers](https://nightlies.apache.org/flink/flink-docs-release-2.2/api/java/org/apache/flink/runtime/rest/handler/job/checkpoints/CheckpointHandlers.html).

## Versioned contract and CI

New producer events include `schema_version: 1`. Missing version on retained
records remains v1; explicit null, wrong type or unsupported version is invalid
and contributes to the bounded `UNSUPPORTED_SCHEMA_VERSION` metric category.
The schema, frozen examples and stricter runtime checks are in `contracts/` and
`tests/test_contract_compatibility.py`. Optional extra fields are tolerated.
The wire schema is a validation aid; runtime additionally checks JSON key
uniqueness, Unicode safety, exact decimal cents and received-time skew.

Install developer dependencies to run all unit tests:

```powershell
python -m pip install -r requirements-dev.txt
python -m unittest discover -s tests -v
```

`.github/workflows/pipeline.yml` runs unit/contract tests, actual Flink validator
and shuffled event-time windows, both Compose/Prometheus configurations and
alert regressions. A separate isolated Docker job tests live Kafka transport,
PostgreSQL schema/reconciliation, and TaskManager crash/replay. It uploads
runtime logs and recovery reports even on failure. No workflow has been pushed
or remotely executed by this change.

## Local verification and limits

All 37 unit tests and connector-plan compilation passed during implementation.
The container Compose overlay and workflow YAML parse successfully. Actual
bounded Flink validator execution passed, and the shuffled/duplicated event-time
window results matched the batch oracle with currency and boundary checks.
Docker Desktop's daemon was unavailable, so image builds, live container
transport and the crash drill have not been executed locally. CI is configured
to perform those checks; its results must be reviewed before claiming measured
recovery or capacity. See [ADR 004](docs/adr/004-runtime-and-contracts.md).
