# Correctness and recovery enhancement

Implemented on 2026-10-03. This change strengthens the existing laptop-based
Kafka → PyFlink → PostgreSQL / DLQ pipeline. It does not require cloud services.

The subsequent [event-time enhancement](EVENT_TIME_BUSINESS_LOGIC.md) adds two
business outputs, migration 002 and a received-time future-clock guard. Apply
both migrations for the current job and use checkpoints from its current graph.

## What changed

| File | Change |
| --- | --- |
| `event_contract.py` | Dependency-free, strict JSON decoding and validation; decimal money; lossless raw payload preservation; all validation errors reported. |
| `producer/event_producer.py` | UUID event IDs, currency, timezone-independent epoch timestamps, keyed records, acknowledged delivery before incrementing counts, configurable rate and invalid-event fraction. |
| `flink-processor/pipeline.py` | Raw-byte Kafka source with source metadata, Python table validator, exhaustive valid/invalid routing, PostgreSQL upserts, explicit DLQ delivery semantics, retained disk checkpoints, restart strategy, explicit restoration. |
| `sql/init.sql` | New-install schema with unique event IDs, currency, original event timestamp and Kafka provenance. |
| `sql/migrations/001_correctness_recovery.sql` | Non-destructive migration for existing databases; historical rows preserved. |
| `docker-compose.yml` | Persistent Kafka data volume, seven-day source retention and broker append timestamps. |
| `.gitignore` | Local checkpoint/savepoint and probe artifacts excluded from Git. |
| `tests/` | Contract tests, actual connector-plan validation, and bounded Flink validator execution. |
| `scripts/recovery_probe.py` | Finite, non-destructive fixtures and PostgreSQL/DLQ reconciliation before and after recovery. |

## Event contract

```json
{
  "event_id": "f08cbb06-eaa5-4c69-9ae1-2d2d12d102fd",
  "user_id": "user_4821",
  "event_type": "payment",
  "timestamp": 1787486400000,
  "amount": 24.50,
  "currency": "MYR"
}
```

- `event_id` and `user_id`: non-blank UTF-8 strings, at most 100 characters,
  without NUL characters. Event IDs must identify immutable events: reuse the
  same ID and payload on retry. The producer generates UUIDs, but the validator
  also permits stable IDs from other producers.
- `event_type`: `food_order`, `ride_request`, `payment`, or `grocery_order`.
- `timestamp`: integer epoch milliseconds, greater than zero and no later than
  `253402300799999`. Original event time is stored as `event_timestamp_ms`.
- `amount`: JSON number from zero to `99999999.99`, representable with two
  decimal places. Strings, booleans, negative/non-finite values, overflow and
  fractional cents are rejected. Parsing uses Decimal, not binary floating point.
- `currency`: three uppercase ASCII letters. This validates format, not
  membership in an ISO currency registry; the simulator emits `MYR`.

Missing or null fields are invalid. Existing Kafka events without `event_id`
and `currency` now go to the DLQ; the pipeline does not invent business identity.
Duplicate JSON keys, invalid UTF-8, malformed JSON and non-object JSON also go
to the DLQ. Kafka null payloads produce `NULL_PAYLOAD`.

## Exhaustive routing and DLQ

The Kafka source uses the `raw` format with a BYTES column. It no longer relies
on `json.ignore-parse-errors`. The validator emits one result for each input.
The valid branch selects `error_reason IS NULL`; the invalid branch selects
`error_reason IS NOT NULL`. This avoids SQL three-valued logic silently excluding
records with null fields.

DLQ records include:

- `record_id`: `topic:partition:offset`, stable when that Kafka record is replayed;
- source topic, partition, offset and Kafka record timestamp;
- `raw_payload_base64`: exact original bytes, including invalid UTF-8;
- `error_reason`: the single validation code, or `MULTIPLE_ERRORS`;
- `validation_errors`: a JSON-encoded list of all failures.

Decode the original payload with `base64.b64decode(record['raw_payload_base64'])`.
Null payload and empty bytes both encode as an empty string; their error codes
distinguish them. Republished corrections get new Kafka coordinates. When
replaying a corrected business event, preserve its original event ID.

## Delivery guarantees

| Component | Guarantee and boundary |
| --- | --- |
| Producer | Counts acknowledged sends only. Retries retain the event ID. Kafka producer retries can still produce duplicate records. |
| Flink | Exactly-once checkpoint consistency for source offsets and managed state; durable filesystem storage. |
| PostgreSQL | JDBC at-least-once delivery with idempotent upserts by `event_id`. Replaying an immutable event leaves one business row. |
| Kafka DLQ | Explicit at-least-once delivery. Recovery can duplicate DLQ messages; consumers can deduplicate by `record_id`. |

The PostgreSQL constraint and Flink sink primary key both use `event_id`.
This is not an atomic transaction across PostgreSQL and Kafka. During recovery,
one output can temporarily lead the other. Reconciliation must use business IDs
and source coordinates rather than comparing raw output counts.

PostgreSQL `processed_at` records the latest write attempt and can change on
replay. `event_timestamp_ms` remains the event time; `ingested_at` is the Kafka
record timestamp. With the Compose broker's `LogAppendTime` default it represents
broker append time. Topics with an explicit timestamp-type override retain that
override. Database timestamps are written in UTC.

Reusing an event ID with a different payload produces an upsert, not a conflict
alert. Enforce immutable IDs in producer contracts. Duplicate sends at different
Kafka offsets can also update provenance to the latest write. SERIAL ID gaps
are expected because PostgreSQL sequences advance during conflict retries.

## Upgrade an existing environment

Stop the producer and pipeline first. Apply the migration before running the
new job; Compose initialization SQL only runs on a fresh database volume.

```powershell
Get-Content -Raw sql/migrations/001_correctness_recovery.sql |
  docker exec -i postgres psql -v ON_ERROR_STOP=1 -U grabuser -d grabevents
```

Historical rows receive `legacy:<id>` identifiers. Unknown historical metadata
stays NULL. Historical duplicates cannot be reliably removed because the old
schema retained neither event identity nor Kafka coordinates. The migration
does not delete rows and can be rerun safely.

For a fresh database, `sql/init.sql` contains the complete new schema. Do not
delete existing volumes to apply this enhancement.

The new Kafka volume does not automatically copy data from an old container's
writable layer. If existing Kafka records matter, back them up or migrate them
before Compose recreates that container. The seven-day retention setting applies
unless a topic has its own override. Single-broker persistence protects container
replacement, not host disk failure.

Checkpoints from the previous pipeline graph are not compatible with the new
raw-source/validator graph. Start the upgraded job without `FLINK_RESTORE_PATH`;
it replays retained source records. Use checkpoints from the upgraded graph for
subsequent recovery.

## Run and recover

Activate the project's Python 3.11 environment, download the existing connector
JARs if needed, and start infrastructure and the job:

```powershell
.venv/Scripts/Activate.ps1
docker compose up -d
python flink-processor/pipeline.py
```

Run the producer in another activated terminal:

```powershell
$env:INVALID_EVENT_RATE = '0'
python producer/event_producer.py
```

By default, checkpoints run every 30 seconds and are stored under
`.flink-state/checkpoints/<job-id>/chk-<n>/`. The job retains three completed
checkpoints, including on cancellation. Transient job failures use ten restart
attempts with ten-second delays and restore the latest completed checkpoint
within the running MiniCluster. Checkpoint timeout is two minutes, with at least
five seconds between checkpoints.

After the entire Python/JVM process exits, restoration is **explicit**, not
automatic. Stop the old job, identify a completed checkpoint belonging to the
upgraded graph, and supply its `_metadata` URI:

```powershell
Get-ChildItem .flink-state/checkpoints -Recurse -Filter _metadata
$snapshot = (Resolve-Path '.flink-state/checkpoints/<job-id>/chk-<n>/_metadata').Path
$env:FLINK_RESTORE_PATH = ([System.Uri]::new($snapshot)).AbsoluteUri
python flink-processor/pipeline.py
```

Replace the placeholders with an actual completed checkpoint. Keep its entire
directory and referenced state files. Restore uses NO_CLAIM and does not silently
ignore unmatched operator state. Explicit operator UID generation is enabled;
reuse the same job graph for checkpoint recovery. Future graph/schema changes
need a separately tested state migration.

Without a restore path, a fresh submission reads from the earliest retained
offset. PostgreSQL remains idempotent, but existing DLQ records can be emitted
again. Clear the restore setting before a deliberately fresh submission:

```powershell
Remove-Item Env:FLINK_RESTORE_PATH -ErrorAction SilentlyContinue
```

`FLINK_CHECKPOINT_DIR`, `FLINK_SAVEPOINT_DIR` and `FLINK_RESTORE_PATH` use absolute
Flink filesystem URIs. Other supported settings are `FLINK_CHECKPOINT_INTERVAL`,
`FLINK_PARALLELISM`, `FLINK_PYTHON_EXECUTABLE`, `KAFKA_BOOTSTRAP`, `SOURCE_TOPIC`,
`DLQ_TOPIC`, `CONSUMER_GROUP`, `POSTGRES_URL`, `POSTGRES_HOST`, `POSTGRES_PORT`,
`POSTGRES_DB`, `POSTGRES_USER`, `POSTGRES_PASS`, `EVENTS_PER_SECOND` and
`INVALID_EVENT_RATE`. Keep JDBC URL and monitoring PostgreSQL settings consistent.

Recovery requires Kafka to retain records since the restored offsets. Retention
expiry, source-topic recreation or loss of the checkpoint disk cannot be repaired
by checkpointing. Shared durable storage and JobManager high availability remain
future cluster improvements. This enhancement retains the local MiniCluster.

## Verify correctness and recovery

Fast checks:

```powershell
python -m unittest discover -s tests -v
python tests/check_flink_plan.py
python tests/check_flink_validator.py
docker compose config --quiet
```

With Kafka, PostgreSQL and the upgraded pipeline running:

```powershell
python scripts/recovery_probe.py --publish
python scripts/recovery_probe.py --verify
```

The probe publishes three valid event IDs, a duplicate of the first event, and
four invalid records: broken JSON, null amount, invalid UTF-8 and null payload.
Verification expects exactly three PostgreSQL business rows with correct values
and all four DLQ source identities with lossless payloads. It tolerates duplicate
DLQ delivery. It does not clear topics or tables.

For a process recovery drill:

1. Complete a checkpoint and note its metadata path.
2. Publish a probe with a new manifest path, then stop the pipeline promptly.
3. Restore from the noted checkpoint and run `--verify` with that same manifest.
4. Repeat verification after another restore to check idempotent database output.

Use `--manifest .flink-state/probe-2.json` for another fixture set. The manifest
must survive the restart. Publishing refuses to overwrite an existing manifest.
For an in-process failure drill, briefly stop PostgreSQL, restart it within the
restart-attempt budget, and reconcile the same manifest once the job recovers.

## Validation performed during implementation

- Ten unittest methods passed, covering missing/null fields, malformed JSON,
  duplicate JSON keys, UTF-8 preservation, invalid identifiers, exact money,
  timestamp/currency validation and deterministic replay.
- Actual Kafka/JDBC/DLQ connector plans compiled with installed PyFlink 2.2.1.
- A bounded Flink job executed the Python validator: every fixture produced one
  output, including invalid UTF-8 and a null payload.
- Python compilation, recovery-probe CLI and Compose configuration checks passed.
- Live PostgreSQL migration, Kafka delivery and failure/restart reconciliation
  were not executed because the Docker daemon was unavailable. Run the probe
  and recovery drill above to validate those integration boundaries.

## References

- [Flink 2.2 raw format](https://nightlies.apache.org/flink/flink-docs-release-2.2/docs/connectors/table/formats/raw/)
- [Flink 2.2 checkpoint and recovery configuration](https://nightlies.apache.org/flink/flink-docs-release-2.2/docs/deployment/config/)
- [Flink JDBC upsert and idempotent writes](https://nightlies.apache.org/flink/flink-docs-stable/docs/connectors/table/jdbc/)
