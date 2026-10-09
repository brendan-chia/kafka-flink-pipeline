# DataLens phase four: evaluation and demonstration

Phase four adds a frozen six-case benchmark, operator-controlled demonstration,
JSON reports and CI. The assistant remains read-only; repair code is confined to
a separately named, harness-owned demo database.

Implementation audit (2026-10-09): the working tree was clean before this work.
Phase one has migration 004, seeded catalogue/lineage, opt-in quality persistence,
payment freshness and exact-decimal comparison. Phase two has typed, bounded
HTTP investigations using one read-only REPEATABLE READ snapshot. Phase three has
the four-node LangGraph, bounded hosted assessment, citation validation, runbook
retrieval and browser interface. These are implemented and covered by unit tests.
Runtime collection still requires explicit enablement and migration; configuration
and source completeness are not automatically verified. Prior documentation records
74 passing tests, with live PostgreSQL, OpenAI and browser checks unavailable.

## Fixed benchmark

The versioned corpus is `tests/fixtures/datalens_benchmark.json`. It freezes event
IDs, raw JSON payloads, delivery timestamps, quality UUIDs, incident range, money
and expected outcomes. Offline investigation IDs and observation times are fixed.
Reports include the corpus SHA-256 and complete returned evidence. Latency and live
provider responses vary; live PostgreSQL freshness is measured at execution time.

| Case | Expected supported outcome |
| --- | --- |
| Late payment | Audit 2 / MYR 0.30; stored 1 / 0.10; mismatch, delta 0.20 |
| Invalid payment payloads | Two INVALID_AMOUNT rejections; quality counters 1 valid / 2 invalid; audit and stored 1 / 0.10 agree |
| Processing interruption | Audit 1 / 0.30; missing aggregate; delta 0.30; job failure unconfirmed |
| Healthy duplicate replay | Two identical deliveries, one unique payment; stored 0.30 agrees |
| Genuinely lower activity | One produced payment, 0.10; fixed external baseline 2 / 0.30; output agrees |
| Missing evidence | No audit, aggregate or history; insufficient evidence |

Constructed fault truth is withheld from model input. The lower-activity baseline
is also withheld: the current assistant does not compare business performance.
Invalid deliveries need not corrupt valid aggregates. The current assistant returns
their quality evidence but does not generate a dedicated invalid-payload diagnosis.

~~~powershell
.\.venv\Scripts\python.exe -m unittest discover -s tests -v
.\.venv\Scripts\python.exe scripts/evaluate_datalens.py --output .flink-state/evaluation-offline.json
.\.venv\Scripts\python.exe scripts/evaluate_datalens.py --postgres --output .flink-state/evaluation-postgres.json
~~~

Default evaluation uses fixture-backed query results, the real event decoder,
comparison classification, evidence service and LangGraph. It executes no SQL or
network calls even when credentials exist. `--postgres` exercises production SQL
in a newly created disposable database, then drops only that database. It requires
CREATEDB on the configured server; it never seeds the configured application database.

The rubric checks diagnostic status/finding, exact monetary/count expectations,
payload rejection reasons, returned quality evidence, resolvable citations, exact
window citation contents and runbook versions/excerpts, retained uncertainty and
the fixed workflow/call sequence. No root cause is established by provenance alone.
Incident cases allow the four conservative mechanisms because the supplied evidence
cannot discriminate among them. Live candidate recall separately measures selection
of the constructed late-arrival / sink-or-recovery targets. Abstention can pass the
supported-diagnosis checks with zero candidate recall. This is not a claim of general
root-cause accuracy; semantic causal support still needs human incident review.

## Optional live-model evaluation

Privately configure `OPENAI_API_KEY` and `DATALENS_OPENAI_MODEL` in the process
environment as in [DATALENS_ASSISTANT.md](DATALENS_ASSISTANT.md). No dotenv loading,
credential printing or secret reporting is added. `--live-model` explicitly opts
into up to six hosted requests. It can be combined with `--postgres`.

~~~powershell
.\.venv\Scripts\python.exe scripts/evaluate_datalens.py --live-model --output .flink-state/evaluation-live.json
~~~

The model receives bounded synthetic evidence and reviewed repository runbooks.
Review runbook content before sending it. Provider token usage is recorded per case.
Set `DATALENS_INPUT_USD_PER_MILLION` and `DATALENS_OUTPUT_USD_PER_MILLION` to your
model's applicable finite nonnegative USD rates to obtain a cost estimate. There are
no embedded prices. Missing rates/usage give unknown cost, never fabricated zero.
Estimates use uncached input/output rates; actual billing may differ. Default offline
execution makes zero model calls. Missing configuration yields a blocked report;
provider refusals/errors or rejected output fail the live run.

Latency is measured with a monotonic clock over graph execution, including collection
and retrieval; database setup/seed time is excluded. Reports include per-case latency
and mean/min/max. Six single samples are not latency percentiles or a service SLA.
Exit codes: 0 passed, 1 failed, 2 blocked. Reports contain no provider exception bodies
or database connection parameters. Setup failures produce blocked reports with a
sanitized exception class; no unsuccessful run is reported as passed.

## Reproducible demonstration

This concise demo reproduces late-payment failure at the database evidence boundary.
It does not claim to inject a Kafka/Flink watermark fault. Each command produces an
independent report: establish baseline → introduce failure → observe/investigate →
operator repairs → independently verify recovery.

For isolated infrastructure, set a private disposable password in
`DATALENS_DEMO_POSTGRES_PASS`, then:

~~~powershell
docker compose -p datalens-phase-four -f docker-compose.datalens-demo.yml up -d --wait
$env:POSTGRES_HOST = '127.0.0.1'
$env:POSTGRES_PORT = '55432'
$env:POSTGRES_DB = 'evaluation_admin'
$env:POSTGRES_USER = 'evaluation'
$env:POSTGRES_PASS = $env:DATALENS_DEMO_POSTGRES_PASS
.\.venv\Scripts\python.exe scripts/demo_datalens.py --setup --expect baseline --output .flink-state/demo-01-baseline.json
.\.venv\Scripts\python.exe scripts/demo_datalens.py --inject-late-payment --expect discrepancy --output .flink-state/demo-02-fault.json
.\.venv\Scripts\python.exe scripts/demo_datalens.py --expect discrepancy --output .flink-state/demo-03-investigation.json
.\.venv\Scripts\python.exe scripts/demo_datalens.py --repair --expect recovered --output .flink-state/demo-04-repair.json
.\.venv\Scripts\python.exe scripts/demo_datalens.py --expect recovered --output .flink-state/demo-05-recovery.json
docker compose -p datalens-phase-four -f docker-compose.datalens-demo.yml down
~~~

Save prior POSTGRES environment settings and restore them afterward. This Compose
file has its own project, loopback port and tmpfs storage, without named volumes.
Stopping it discards only disposable demo data; it never invokes `down -v`.
On another PostgreSQL server, `--setup` refuses an existing database. Subsequent
actions require the ownership marker and a `datalens_demo_` name. Use a new
`--database datalens_demo_run2` for another run; pass it to every command. No automatic
cleanup drops persistent demo databases. Repairs are scoped to MYR and the fixed
five-minute window, and require an observed discrepancy. Investigations never repair.
`--live-model` optionally adds hosted assessment to an individual demo command.

For a real processing interruption, the existing isolated runtime/recovery path in
[CORRECTNESS_AND_RECOVERY.md](CORRECTNESS_AND_RECOVERY.md) and
[OPERATIONAL_DASHBOARDS_AND_BENCHMARKS.md](OPERATIONAL_DASHBOARDS_AND_BENCHMARKS.md)
uses `scripts/recovery_drill.py --inject-taskmanager-failure`. That drill deliberately
kills and starts the opt-in TaskManager, verifies checkpoint restore, audit and
duplicate replay, and requires its documented 10-minute checkpoint interval. Run it
only on the disposable runtime described there. Phase-four fixture interruptions
exercise diagnostic behavior; they are not a substitute for that integration drill.

CI (`.github/workflows/datalens-evaluation.yml`) runs all unit tests, offline and
real PostgreSQL benchmarks, then the five demo stages against an ephemeral service.
JSON artifacts upload even on failure. Existing pipeline/recovery CI is preserved.
Hosted-model evaluation remains explicitly opt-in and is never run on pull requests.

## Measured local verification

On 2026-10-09, 82 unit tests passed (eight new evaluation tests). All six fixed cases
passed all seven deterministic check categories. The final run averaged 42.26 ms
(35.176–46.192 ms), with unit checks running concurrently; this is fixture graph
latency only. Zero model calls incurred zero hosted cost. Compilation, dependency
consistency, demo Compose configuration and Git whitespace checks passed. The real
SDK was tested with offline transport for usage capture.

PostgreSQL benchmark and demo returned blocked OperationalError reports. Docker's
daemon was unavailable. Hosted-model evaluation returned blocked because key/model
configuration was absent, so live accuracy, token cost and latency remain unmeasured.
These checks must still run in a service-enabled environment. Machine-readable
verification is retained in [docs/evaluation/phase-four-results.json](docs/evaluation/phase-four-results.json);
complete local reports are under `.flink-state/` and CI uploads reports as artifacts.
