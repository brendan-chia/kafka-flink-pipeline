# Event-time business logic

Implemented on 2026-10-03. This adds stateful business analytics to the existing
validation, PostgreSQL audit and DLQ pipeline. It uses the event's original
`timestamp`, not processing time or Kafka arrival time, to assign windows.

## Outputs

| Table | Meaning | Key |
| --- | --- | --- |
| `processed_events` | Existing valid-event audit, including late arrivals | `event_id` |
| `payment_revenue_windows` | Payment count and simulated gross payment revenue per UTC window and currency | Window start/end, currency |
| `activity_windows` | Activity count per UTC window, event type and currency | Window start/end, event type, currency |

Revenue includes only the `payment` event type. Food and ride amounts are not
added to payment revenue. Currencies are never summed together. This simulator
does not yet model refunds, payment failures, merchant joins or order lifecycles.

## Window and watermark policy

- Default windows are five minutes, aligned to UTC epoch boundaries, e.g.
  `[12:00:00, 12:05:00)`. An event exactly at 12:05 belongs to the next window.
- Watermarks trail observed valid event time by ten seconds. This permits some
  out-of-order arrival; it is not a ten-second delay applied to each record.
- A window emits a final result when the watermark passes its closing boundary.
  PostgreSQL upserts that final result using the window/group key.
- `ROW_NUMBER` window deduplication counts an immutable event ID once per window.
  Deduplication state is cleared when the window closes. A second event-time
  window aggregates that deduplicated output using its `window_time` attribute.
- The configured source idleness timeout is 60 seconds, allowing inactive inputs
  to stop holding back watermarks. Watermark assignment occurs after the Python
  computed timestamp; do not assume independent per-Kafka-partition clocks in
  this design. Multiple active parallel inputs can still hold back progress.
- An entirely quiet stream does **not** advance event time with wall-clock time.
  The final open window waits for later events or a bounded-input completion.

Example: a 12:04 payment received after a 12:05:20 event can miss the closed
12:00–12:05 window. It remains in `processed_events`, but the finalized streaming
revenue is not automatically revised. The reconciliation SQL below includes it.

## Invalid timestamps and future-clock protection

Strict validation runs before timestamps can influence the business clock.
Flink's watermark operator rejects null rowtime, so invalid records receive
epoch zero as an internal sentinel and are excluded from business windows.
They retain the existing lossless DLQ treatment.

Events more than 60 seconds ahead of their Kafka record timestamp are quarantined
with `FUTURE_EVENT_TIME`. Both the timestamp extractor and the routing validator
use the same rule. This prevents an otherwise valid year-9999 event from closing
ordinary windows prematurely. Kafka's Compose broker uses `LogAppendTime`; if a
topic overrides that to producer-supplied timestamps, this guard inherits that
clock and is weaker. Older event times have no fixed maximum arrival delay.

## Changes made

- `flink-processor/pipeline.py`: event-time computed column, periodic watermarks,
  idle-input configuration, window deduplication, two business queries and JDBC
  upsert sinks. The existing audit and DLQ outputs remain in the StatementSet.
- `event_contract.py`: optional received-time validation and future-time guard.
- `producer/event_producer.py`: delayed timestamp and duplicate injection knobs,
  plus a seed for reproducible random choices.
- `sql/init.sql`: tables for new database installations.
- `sql/migrations/002_event_time_business.sql`: tables for existing databases;
  repeatable and non-destructive.
- `sql/reconcile_event_time_windows.sql`: explicitly rebuilds selected completed
  windows from unique audit rows, including late data.
- `tests/check_event_time_windows.py`: real Flink execution compared with a batch
  oracle after shuffle/duplicate injection, with currency and boundary checks.
- `tests/check_late_event_policy.py`: continuous-source test that observes a
  closed window before publishing a late payment, then verifies audit retention
  and unchanged finalized revenue.
- `tests/check_event_time_sql.py`: actual PostgreSQL schema, repeatable migration
  and reconciliation verification in an isolated schema, rolled back afterward.
- Existing contract and plan tests updated for the new outputs and timestamp rule.

## Apply the new migration

Stop the producer and Flink pipeline. If migration 001 has not yet been applied,
follow `CORRECTNESS_AND_RECOVERY.md` first. Then run from the project directory:

```powershell
Get-Content -Raw sql/migrations/002_event_time_business.sql |
  docker exec -i postgres psql -v ON_ERROR_STOP=1 -U grabuser -d grabevents
```

Successful output ends in `COMMIT`. Existing audit rows are not modified.
Fresh database volumes receive these tables through `sql/init.sql`.

The graph changed, so do not restore a checkpoint from the previous routing-only
job. For the first start of this version:

```powershell
Remove-Item Env:FLINK_RESTORE_PATH -ErrorAction SilentlyContinue
python flink-processor/pipeline.py
```

It replays retained Kafka records. Historical windows can close before their
late records are replayed; reconcile historical ranges after replay if complete
historical totals are required. PostgreSQL audit rows remain idempotent; the DLQ
can repeat records. Subsequent restores must use this new graph and the same
window/watermark configuration. Configuration changes can change state semantics
even when generated operator IDs match.

## Exercise the producer

In another activated Python environment:

```powershell
$env:INVALID_EVENT_RATE = '0'
$env:DELAYED_EVENT_RATE = '0.25'
$env:MAX_EVENT_DELAY_SECONDS = '20'
$env:DUPLICATE_EVENT_RATE = '0.10'
$env:EVENT_RANDOM_SEED = '42'
python producer/event_producer.py
```

Delayed events are backdated before publishing; the producer does not sleep for
the simulated delay. Duplicates copy the preceding valid event's ID, timestamp
and payload. The seed fixes random choices, not UUIDs or wall-clock timestamps.
Defaults keep delay and duplicate injection off.

Inspect results after the first window closes:

```powershell
docker exec postgres psql -U grabuser -d grabevents -c "SELECT * FROM payment_revenue_windows ORDER BY window_start DESC LIMIT 10;"
docker exec postgres psql -U grabuser -d grabevents -c "SELECT * FROM activity_windows ORDER BY window_start DESC LIMIT 10;"
```

For a quicker demo, set `$env:EVENT_WINDOW_SECONDS = '10'` in the pipeline
terminal **before** starting a fresh job. Production-style default is 300 seconds.
Do not run the regression tests below with that override set; their oracle uses
the five-minute default. Clear it with `Remove-Item Env:EVENT_WINDOW_SECONDS`.

## Reconcile finalized windows with late arrivals

Stop the Flink job first. Choose a UTC range aligned to the configured window
size and covering only completed historical windows. For the default size:

```powershell
Get-Content -Raw sql/reconcile_event_time_windows.sql |
  docker exec -i postgres psql -v ON_ERROR_STOP=1 -U grabuser -d grabevents `
    -v "from_utc=2026-10-03 00:00:00" -v "until_utc=2026-10-03 01:00:00" `
    -v "window_seconds=300"
```

The script uses a consistent database snapshot and replaces aggregate values
for groups present in the selected audit range. It does not increment existing
sums or delete rows. Boundaries must be aligned; SQL constraints reject invalid
ranges. Use your actual dates and window size. Keep all writers stopped while
correcting results; replaying a job can overwrite corrected aggregates, so run
reconciliation again after such a replay.

Historical rows from before migration 001 have no event timestamp or currency
and cannot participate. Reconciliation cannot reconstruct data absent from the
audit table. Immutable event IDs and timestamps are required: an ID reused with
a different timestamp can appear in multiple streaming windows despite the
single audit upsert. Corrections that remove/change a historical group require
a separate reviewed repair; this script does not delete stale groups.

## Configuration

| Setting | Default | Purpose |
| --- | --- | --- |
| `EVENT_WINDOW_SECONDS` | `300` | UTC tumbling window size, 1–86400 seconds |
| `EVENT_WATERMARK_SECONDS` | `10` | Out-of-order tolerance, 0–86400 seconds |
| `EVENT_IDLE_SECONDS` | `60` | Inactive-input timeout |
| `EVENT_MAX_FUTURE_SKEW_SECONDS` | `60` | Maximum lead over Kafka record timestamp |
| `DELAYED_EVENT_RATE` | `0` | Producer probability of a backdated event |
| `MAX_EVENT_DELAY_SECONDS` | `20` | Maximum producer backdating |
| `DUPLICATE_EVENT_RATE` | `0` | Producer probability of replaying the previous valid event |
| `EVENT_RANDOM_SEED` | unset | Seed for random producer choices |

## Verification

Use the project Python 3.11 environment:

```powershell
python -m unittest discover -s tests -v
python tests/check_flink_plan.py
python tests/check_flink_validator.py
python tests/check_event_time_windows.py
python tests/check_late_event_policy.py
python tests/check_event_time_sql.py
```

The SQL check needs the running `postgres` container. The other checks use
local Flink and bounded/monitored files rather than live Kafka. The late-policy
test can take several minutes with the production checkpoint interval because
the Flink collector exposes results after successful checkpoints.

Validation performed: contract tests, four-output connector plan compilation,
actual Flink windows matching a batch oracle, a live closed-window late-data
test, and actual PostgreSQL schema and late-inclusive reconciliation.
The PostgreSQL test rolled back all its changes;
it did not apply migration 002 to your public tables. Live Kafka transport was
not exercised; the executed Flink tests used filesystem sources.

## References

- [Flink window deduplication](https://nightlies.apache.org/flink/flink-docs-release-2.2/docs/sql/reference/queries/window-deduplication/)
- [Flink event-time attributes](https://nightlies.apache.org/flink/flink-docs-release-2.2/docs/dev/table/concepts/time_attributes/)
