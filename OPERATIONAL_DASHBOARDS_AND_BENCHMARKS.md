# Operational dashboards and benchmarks

Implemented on 2026-10-03 as the next step after event-time business logic.

## What changed

- Grafana automatically provisions **Payments pipeline operations** in the
  **Flink Pipeline** folder, using a stable Prometheus datasource UID.
- Sixteen panels cover scrape/collector health, observed input throughput,
  five-minute invalid fractions and bounded reasons, processing latency, audit
  freshness, checkpoint duration/failures, backpressure, watermark age and
  checkpoint-committed Kafka consumer lag.
- Normal producer traffic now defaults to **zero invalid events**. Set
  `PRODUCER_PROFILE=fault` for 20% invalid traffic. An explicit
  `INVALID_EVENT_RATE` overrides either profile.
- Alerts use recent source quality rather than the old cumulative DLQ fraction.
  New alerts detect stale audit output with recent valid input, checkpoint failures
  and sustained backpressure. Alertmanager keeps its existing local UI receiver.
- A finite benchmark publishes valid unique records with 128 simulated user keys,
  waits for broker acknowledgements, then reconciles IDs and business values in
  PostgreSQL. It does not clear topics, tables, or previous results.

## Start and apply

Apply migrations 001 and 002 first if needed, following their existing guides.
Migration 003 adds only a repeatable index for recent audit queries:

```powershell
Get-Content -Raw sql/migrations/003_operational_metrics.sql |
  docker exec -i postgres psql -v ON_ERROR_STOP=1 -U grabuser -d grabevents
docker compose up -d prometheus grafana alertmanager kafka-exporter postgres-exporter
```

Restart the Python pipeline to load the new metric collector. The streaming
graph has not changed in this enhancement; preserve the same event-time settings
and follow the existing restore procedure when restoring state.
Grafana reads the dashboard JSON from its existing provisioning mount; allow
its provisioning scan to complete or restart Grafana. Open
<http://localhost:3000/d/grab-pipeline-operations> (`admin` / `admin`).
Prometheus loads its rules at startup; if already running, use
`docker compose restart prometheus`.

In a separate producer terminal:

```powershell
$env:PRODUCER_PROFILE = 'normal'
Remove-Item Env:INVALID_EVENT_RATE -ErrorAction SilentlyContinue
python producer/event_producer.py
```

Set `PRODUCER_PROFILE` to `fault` and restart the producer to exercise the
invalid-rate alert. After five minutes of observer coverage and two minutes
above threshold, it fires if the window has at least 20 observed records.
Return to `normal` to recover. Fault profile is intentionally expected to alert.

## Metric semantics and limits

| Metric | Meaning |
| --- | --- |
| `pipeline_input_events_observed_total{outcome}` | Counter of source deliveries independently classified since observer start; includes duplicates |
| `pipeline_input_events_window{outcome}` | Observed deliveries whose broker timestamps fall in the last 300 seconds |
| `pipeline_invalid_fraction_window` | Invalid / all source deliveries in that window; NaN when empty |
| `pipeline_invalid_events_window{reason}` | Invalid records by bounded primary error; multiple failures count once as `MULTIPLE_ERRORS` |
| `pipeline_observer_coverage_start_timestamp_seconds` | Last source observer start/reset; recent windows are partial for 300 seconds afterward |
| `pipeline_audit_latency_seconds{clock,quantile}` | Five-minute audit population p50/p95/p99 from event or broker timestamp to Flink processing timestamp |
| `pipeline_audit_freshness_seconds` | Age of latest processing timestamp; NaN for empty audit |
| `pipeline_audit_recent_rows` | Audit rows processed in the last five minutes |
| `pipeline_metrics_collection_success{source}` | Latest PostgreSQL, source observer or DLQ collection success |
| `pipeline_metrics_last_collection_timestamp_seconds{source}` | Last successful source collection; check alongside gauges that may hold stale values |

The source observer uses the **same validator and future-clock rule** as Flink.
It starts at current partition end offsets, never joins the Flink group, and
never commits offsets. It measures input quality, not successful DLQ delivery.
Observer failure resets coverage and resumes from current end offsets; missed
records are not reconstructed. Restart the pipeline after adding source topic
partitions so the observer discovers them. At high rates its Python parsing,
five-minute in-memory record window and PostgreSQL percentile queries add
overhead; include this overhead when benchmarking.

Kafka Compose uses `LogAppendTime`. Producer-supplied topic timestamps or clock
skew weaken recent-window interpretation. The database queries treat stored
naive timestamps as UTC; negative latency samples are excluded, not clamped.
Event latency includes intentional delays and replay. Processing timestamp is
assigned **before JDBC commit**, so the dashboard does not measure database
visibility latency. The benchmark does, with polling resolution recorded.
Legacy rows lacking timestamps cannot provide all latency samples.

Audit count and DLQ end offsets remain gauges with different populations:
unique IDs versus replay deliveries. `pipeline_dlq_rate` is retained for
compatibility, but neither alerts nor the new dashboard use it as a recent rate.
DLQ end offsets are not the number of records retained after Kafka retention.

Native runtime names follow the [Flink 2.2 metrics reference](https://nightlies.apache.org/flink/flink-docs-release-2.2/docs/ops/metrics/)
and [Prometheus reporter](https://nightlies.apache.org/flink/flink-docs-release-2.2/docs/deployment/metric_reporters/#prometheus).
Check the live `/metrics` endpoints if a panel has no data. An unused MiniCluster
reporter port may show down. Watermark sentinels are filtered, and an idle stream
can show rising watermark age without a fault. Kafka lag uses committed group
offsets, which can trail processing until a checkpoint completes.

## Benchmark procedure

Start Kafka, PostgreSQL and the pipeline, with host ports reachable. Stop the
continuous producer for an interpretable benchmark. Use the same `SOURCE_TOPIC`
and PostgreSQL environment settings in both terminals. A dedicated benchmark
topic/group is preferable; create the source topic before starting a fresh job
and keep its input retention long enough. A fresh job reads retained data, so
wait until existing backlog is drained before starting a run.

No-network preview:

```powershell
python scripts/benchmark_pipeline.py --dry-run --rate 20 --duration 30 --output .flink-state/benchmarks/preview.json
```

Run several input rates, with a new output filename each time:

```powershell
python scripts/benchmark_pipeline.py --rate 2 --duration 30 --output .flink-state/benchmarks/p1-r2.json --hardware-notes 'Record actual RAM and Docker limits here'
python scripts/benchmark_pipeline.py --rate 20 --duration 30 --output .flink-state/benchmarks/p1-r20.json --hardware-notes 'Record actual RAM and Docker limits here'
python scripts/benchmark_pipeline.py --rate 100 --duration 30 --output .flink-state/benchmarks/p1-r100.json --hardware-notes 'Record actual RAM and Docker limits here'
```

For parallelism comparisons, stop the job, clear `FLINK_RESTORE_PATH`, set
`FLINK_PARALLELISM=2` **in the pipeline terminal**, and start a fresh job against
a source topic with at least two partitions. Set the same environment variable
in the benchmark terminal and use `p2-...` filenames. The report records
**declared** parallelism; it does not query the job to verify it. Do not restore
the old checkpoint merely to change parallelism; this PyFlink graph's restore
compatibility is not established. Keep this exercise separate from recovery drills.

The report contains hardware notes, software versions, seeded workload settings,
acknowledged IDs/offsets, missing/duplicate/mismatched IDs, achieved publication
rate, reconciled throughput, and p50/p95/p99 first database visibility latency.
First visibility is observed every 250 ms by default (`--poll-interval`). The
latency starts before Kafka send, so it includes producer buffering and sink
flushes. The runner bounds pending sends to 64; achieved rates below target
can reflect producer or broker limits, not just Flink. The random seed fixes
payload choices, while run IDs and wall-clock timestamps differ each run.

Exit code is zero only on successful reconciliation or explicit dry run.
Timeouts, collector failures and mismatches produce failure reports. Failed
sends may still reach Kafka even without an acknowledgement; such uncertain
deliveries are not counted as confirmed expected IDs. Inspect the dashboard
throughout each run and record lag, checkpoint duration, backpressure and CPU.
Repeat runs after warmup, keeping configuration and background workload stable.
These audit benchmarks do not prove closed-window revenue correctness or
exactly-once DLQ delivery; use the existing event-time and recovery tests for those.

## Incident runbooks

### Stalled processing

1. Check scrape and collection health first; stale telemetry is not proof of a stalled job.
2. Confirm the producer is running and recent valid-source counts are increasing.
3. Compare committed Kafka lag, audit freshness and Flink backpressure. A quiet
   stream or replay of existing IDs can explain the audit alert.
4. Inspect pipeline logs for JDBC errors, checkpoint failures or job restarts.
   If the sink is reachable but slow, lower input rate and examine PostgreSQL
   sessions before increasing parallelism.
5. After recovery, run ID reconciliation and watch freshness and lag recover.
   Do not delete source data or checkpoint directories to clear an alert.

### PostgreSQL unavailable

1. Check `docker compose ps postgres`, container logs and the configured host port.
   A local PostgreSQL process can occupy 5432; confirm which server you reach.
2. Compare PostgreSQL exporter health with the application's PostgreSQL
   collector. Authentication failures require matching configured credentials,
   not volume deletion.
3. Restore connectivity, then let Flink's retries/restart policy work. If a manual
   restart is necessary, use the documented compatible checkpoint restore.
4. Reconcile acknowledged event IDs. Watch audit catch-up and recent latency;
   replay can temporarily inflate event latency and DLQ end offsets.

### Failed checkpoints or recovery

1. Check the Flink error log, checkpoint duration/failure panels and sink health.
2. Confirm `.flink-state` storage exists, has free space and remains writable.
3. Identify the last completed checkpoint for the same graph and event-time
   configuration; follow [CORRECTNESS_AND_RECOVERY.md](CORRECTNESS_AND_RECOVERY.md).
4. Run `scripts/recovery_probe.py` verification after restart. Record last good
   checkpoint, incident duration, missing IDs and reconciliation outcome.
5. An incompatible restore requires a deliberate fresh replay followed by
   historical window reconciliation, as described in the event-time guide.

## Verification

```powershell
python -m unittest discover -s tests -v
python tests/check_flink_plan.py
python tests/check_operational_sql.py
# For a PostgreSQL container without a published host port:
python tests/check_operational_sql.py --docker
docker run --rm --entrypoint /bin/promtool -v "${PWD}/monitoring:/etc/prometheus:ro" prom/prometheus:v2.47.0 check config /etc/prometheus/prometheus.yml
docker run --rm --entrypoint /bin/promtool -v "${PWD}/monitoring:/etc/prometheus:ro" prom/prometheus:v2.47.0 test rules /etc/prometheus/prometheus_alerts_test.yml
```

Verified: unit tests, four-output Flink plan compilation, actual PostgreSQL
collector SQL (empty and recent populations), repeatable index migration,
Prometheus configuration/rule syntax and synthetic alert behavior, plus
benchmark dry run. PostgreSQL test changes were rolled back.

No end-to-end capacity benchmark or live Grafana rendering was performed in
this session: only PostgreSQL was running, with no published host port. The
host's separate PostgreSQL rejected the project's credentials. No throughput
or latency results are claimed, and migration 003 was not applied to public tables.
