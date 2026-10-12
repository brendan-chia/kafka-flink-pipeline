# Phase 3 - Customer profiles and serving

Status: implemented. Requires Phase 2's transport envelope. Phase 4 segments remain unimplemented.

## Setup and migrations

Run from the repository root with Docker Desktop's Linux engine running:

```powershell
docker compose -f docker-compose.yml -f docker-compose.journeypulse.yml config --quiet
docker compose -f docker-compose.yml -f docker-compose.journeypulse.yml up -d kafka postgres
docker compose -f docker-compose.yml -f docker-compose.journeypulse.yml run --rm --build journeypulse-bootstrap
docker compose -f docker-compose.yml -f docker-compose.journeypulse.yml run --rm journeypulse-bootstrap check
docker compose -f docker-compose.yml -f docker-compose.journeypulse.yml up -d --build journeypulse-gateway journeypulse-customers journeypulse-processor
```

Bootstrap applies `001_foundation.sql` and additive `002_profiles.sql` inside the existing serialized database transaction. Rerunning preserves rows and migration timestamps. Rerun bootstrap on existing volumes; initialization scripts alone do not migrate them. `check` requires both migrations. Contract version remains 1. Public and DataLens tables are untouched.

Customer API: http://127.0.0.1:8021/docs. Dedicated Flink REST/UI: http://127.0.0.1:8082. Gateway remains on 8020. `/health/live` checks the API process; `/v1/health/ready` checks database access and migration 2. The foreground processor submission is not automatically resubmitted, preventing accidental second jobs.

Host API:

```powershell
.\.venv\Scripts\python.exe -m pip install -r requirements-journeypulse-gateway.txt
.\.venv\Scripts\python.exe scripts/journeypulse_customers.py
```

Host processing requires Java 17+, the Kafka connector from `download_jars.sh`, and `requirements-journeypulse-processor.txt`. Run `python scripts/journeypulse_processor.py` or submit with `flink run --python`. Linux/Compose is the supported live runtime; Windows host graph construction is checked.

## Authorization and customer API

Customer tokens use audience `journeypulse-customers`; tracking tokens use `journeypulse-events`. Each API rejects the other's tokens. Both bind source, user and session, using the existing source registrations/signing key. Reads and updates compare the path user to verified claims. Onboarding derives user identity only from claims and rejects body-supplied IDs.

A trusted application backend calls the customer API's `POST /v1/sessions` with `X-Source-Key` and `{source_id,user_id,session_id}`. It must derive identity from its authenticated application context, as in Phase 2. Source keys must never reach browsers. Session issuance rejects Origin headers; bearer endpoints enforce the registered source origins. Source-key possession is a trusted backend capability to choose identity; JourneyPulse does not provide end-user login or tenant namespaces. User IDs must be globally unique across sources. Rotate development keys for deployment. Rate budgets remain process-local.

Local PowerShell example using the configured development source:

```powershell
$sourceKey = if ($env:JOURNEYPULSE_SOURCE_KEY) { $env:JOURNEYPULSE_SOURCE_KEY } else { 'journeypulse-local-source-key' }
$identity = @{ source_id='demo-app'; user_id='u123'; session_id='s456' } | ConvertTo-Json
$session = Invoke-RestMethod -Method Post -Uri http://127.0.0.1:8021/v1/sessions -ContentType application/json -Headers @{ 'X-Source-Key'=$sourceKey } -Body $identity
$headers = @{ Authorization="Bearer $($session.access_token)" }
$attributes = @{ preferred_language='ms'; marketing_consent=$true } | ConvertTo-Json
Invoke-RestMethod -Method Post -Uri http://127.0.0.1:8021/v1/customers -ContentType application/json -Headers $headers -Body $attributes
Invoke-RestMethod -Uri http://127.0.0.1:8021/v1/customers/u123 -Headers $headers
$revoke = @{ marketing_consent=$false } | ConvertTo-Json
Invoke-RestMethod -Method Patch -Uri http://127.0.0.1:8021/v1/customers/u123 -ContentType application/json -Headers $headers -Body $revoke
```

`POST /v1/customers` onboards/upserts the token user. `preferred_language` is required (`en`, `ms`, `en-GB`); optional `marketing_consent` must be a JSON boolean. New customers default to consent false; repeated onboarding preserves omitted consent. `PATCH /v1/customers/{user_id}` preserves omitted attributes and rejects nulls, unknown fields and empty patches. Consent changes record `consent_updated_at`; attribute writes record `attributes_updated_at`. Neither endpoint can change behavior.

Events before onboarding create a readable placeholder with null language/onboarding timestamps and false consent. Later onboarding fills attributes without resetting behavior. `marketing_eligible` is true only for an onboarded customer with explicit true consent; this is a consent gate, not a Phase 4 segment. Behavioral processing never updates attributes or consent.

| Status | Meaning |
| --- | --- |
| 200 | Successful read/onboarding/update |
| 401 | Missing, invalid, expired or wrong-audience token/source credential |
| 403 | Identity mismatch, disallowed origin or browser token issuance |
| 404 | Customer absent; a placeholder counts as present |
| 422 | Invalid attributes/identity; consent must be boolean |
| 429 | Source budget exhausted; Retry-After supplied |
| 503 | Database unavailable or migration missing; errors sanitized |

Responses use `Cache-Control: no-store`. Updating an unknown customer returns 404; use onboarding to create it.

## Separate processor and durable guarantees

The job reads only `journeypulse-events`, keyed by user, using group `journeypulse-profiles-v1`. It validates transport version 1, the nested event and the Kafka key. Occurrence time is `event.occurred_at`; gateway `received_at` is separate metadata.

Checkpoint interval is 10 seconds, with fixed-delay restart (10 attempts, 10 seconds) and retained externalized checkpoints. Compose uses its own `journeypulse_state` volume under `/opt/flink/journeypulse-state`; host defaults use `.journeypulse-state`. Configure `JOURNEYPULSE_CONSUMER_GROUP`, `JOURNEYPULSE_PARALLELISM`, `JOURNEYPULSE_CHECKPOINT_DIR`, `JOURNEYPULSE_SAVEPOINT_DIR` and `JOURNEYPULSE_RESTORE_PATH` for other runtimes. Submission and workers must share configuration and durable storage. The implementation uses [Flink's Table/DataStream integration](https://nightlies.apache.org/flink/flink-docs-release-2.2/docs/dev/python/datastream/intro_to_datastream_api/) to carry Kafka metadata into the transactional writer.

Fresh jobs start at earliest offsets. Automatic task recovery restores checkpointed source offsets. To explicitly restore, set `JOURNEYPULSE_RESTORE_PATH` to checkpoint/savepoint metadata and resubmit after stopping the previous job. A fresh resubmission without restore replays retained history safely. Consumer group commits are not the persistence correctness authority.

Each record commits one PostgreSQL transaction:

1. Insert a permanent receipt keyed by topic/partition/offset. An existing matching receipt returns `replay` without updating totals or processing time. Reused coordinates with changed bytes fail the job.
2. Lock the global event-ID decision. A retained ID with identical canonical event plus source returns `duplicate`; changed event/source returns `conflict`. Request ID and receipt timestamp are excluded from the event fingerprint. First committed payload wins; receipt outcomes preserve the rejection evidence.
3. Create a placeholder if needed and lock the behavioral row. Completed orders insert permanent `(user_id,order_id)` identities, retaining first committed occurrence/restaurant/cart facts.
4. Update event counts, distinct order count, occurrence/receipt maxima, processing time and durable revision only for a new event contribution. Commit receipts, identities and summary together.

Counters increment only behind durable uniqueness in that same transaction. A crash after database commit and before checkpointing cannot repeat the increment. Concurrent decisions serialize through event/customer locks. No processor snapshot is written, so replay of an old checkpoint cannot overwrite newer durable state. Latest activity uses maximum occurrence time; arbitrarily late valid events contribute without moving it backwards. No watermark discards late behavior.

`event_counts.order_completed` counts event notifications under the event-ID retention rules. `completed_orders` counts distinct orders permanently. A new event ID about an existing order adds a notification, not an order. `latest_order_at` is the maximum first accepted occurrence time across distinct orders; later notifications do not rewrite order facts. `latest_activity_at` reflects all accepted notification occurrence times.

Malformed envelopes, invalid events and mismatched keys receive an `invalid` receipt and no behavior. Their original record remains in Kafka; the processor does not republish gateway quarantine. Fixed Flink operator metrics/log outcomes are `applied`, `duplicate`, `conflict`, `invalid`, `replay`, without customer labels. Database failures propagate and fail the task; uncommitted writes cannot be skipped by a successful checkpoint. Lost commit acknowledgement is safe to retry.

This is an at-least-once external sink with replay-safe durable effects, not an atomic distributed Kafka/PostgreSQL transaction. Existing transaction processing and DataLens remain independent.

## Retention and limitations

Event-ID retention is **seven days from first database processing**, not occurrence or receipt time. Duplicates do not extend expiry. Expired IDs are replaced on lookup and pruned approximately every minute while traffic flows. Idle cleanup requires traffic or `ProfileStore().prune_event_ids()`. This bounds time coverage, not a fixed number of IDs.

After expiry, the same ID at a **new Kafka offset** can count again, and conflict detection for that ID ends. The **same offset** remains a no-op forever while its receipt exists. Order IDs remain durable outside this window and continue protecting order totals.

Receipts/orders have no automatic deletion and grow with traffic. Do not prune them while replay is possible. Topic recreation can reuse offsets and is unsupported with the same ledger namespace; provision a coordinated new namespace/migration first. Back up summaries, receipts, IDs and orders consistently. Losing receipts invalidates replay guarantees. Seven-day Kafka retention can lose unprocessed data during longer outages.

The sink uses synchronous per-record transactions/connections; throughput and latency SLOs are not established. Local shared credentials and one broker provide logical isolation. Optional administrator-run `journeypulse/sql/roles.sql` creates NOLOGIN capability roles: processing can insert placeholder IDs/change behavior but cannot update attributes; serving can read behavior/change attributes but cannot mutate behavior. Grant these to dedicated login roles and configure Compose's `JOURNEYPULSE_PROCESSOR_USER/PASSWORD` and `JOURNEYPULSE_SERVING_USER/PASSWORD`. Bootstrap retains owner credentials. The optional role script requires CREATEROLE/object ownership and is not run automatically.

## Response timestamps and freshness

`GET /v1/customers/{user_id}` returns attributes/consent, creation/onboarding/attribute timestamps, all five `event_counts` (zero when absent), distinct `completed_orders`, `latest_activity_at`, `latest_order_at`, `latest_received_at`, `revision` and `served_at`. `freshness` contains:

- `processed_at`: last committed new behavioral contribution; duplicates/replays do not refresh it.
- `summary_age_seconds`: time since that contribution; null for no behavior.
- `receipt_to_processing_seconds`: last processing time minus maximum receipt time, clamped to zero. This is a summary observation, not per-event latency or a percentile.
- `status`: `never_processed`, `recent`, or `stale` (age over 300 seconds).
- `stale_after_seconds` and `scope`: threshold and `customer_summary_age_not_consumer_lag`.

An inactive customer's summary may be old while processing is healthy. These fields do not prove the consumer has no backlog. Attribute writes do not fabricate behavioral freshness. Dates are UTC; occurrence and processing times are separate.

## Verification

Offline:

```powershell
.\.venv\Scripts\python.exe -m unittest discover -s tests -p 'test_journeypulse*.py' -v
node --test journeypulse/sdk/journeypulse.test.mjs
.\.venv\Scripts\python.exe tests/check_journeypulse_plan.py
docker compose -f docker-compose.yml -f docker-compose.journeypulse.yml config --quiet
```

The graph check needs Java/PyFlink/the connector but does not contact Kafka/PostgreSQL. Offline cursor fixtures test transaction guards/failure propagation; they do not establish real database concurrency or runtime recovery.

Live after bootstrap/startup:

```powershell
.\.venv\Scripts\python.exe tests/check_journeypulse_profiles_db.py
.\.venv\Scripts\python.exe tests/check_journeypulse_profiles.py --restart
```

The real database suite covers duplicates, late events, repeated orders, both onboarding orders, consent preservation, conflicts, expiry, replay, concurrency, invalid records and rollback after an injected partial-transaction failure. It also exercises the API against real PostgreSQL. After applying `roles.sql`, set `JOURNEYPULSE_CHECK_ROLES=1` to verify processor/serving privilege separation. It writes unique test users and synthetic partition-99 receipts. The HTTP probe sends a five-event journey through the gateway, duplicates it, adds late/repeated-order notifications, onboards the placeholder, checks attributes/authorization and reports measured event-to-profile elapsed seconds. `--restart` cancels only the dedicated JourneyPulse job after a completed checkpoint, resubmits from earliest offsets, waits for actual replay metrics, then checks unchanged totals/revision/processing time. Use this option on the dedicated local Compose cluster. Probes retain their test evidence.

[CI](../../.github/workflows/journeypulse.yml) runs live probes, SDK regression, repeated migrations, Compose validation and actual graph construction. Local evidence: 141 Python regression tests and 16 SDK tests passed, the actual Flink graph built, migrations/bootstrap/check ran repeatedly, and 12 real PostgreSQL correctness/API/privilege tests passed on an isolated temporary PostgreSQL 18 instance. The existing host database was left untouched because project credentials were rejected. Docker's Linux engine and Kafka were unavailable, so the full gateway/Kafka/Flink integration, actual checkpoint recovery and measured event-to-profile freshness remain unverified locally. CI targets the project's PostgreSQL 15 container. No live freshness measurement is claimed.

Next: [Phase 4 - Segments and targeting](phase-4-segments.md).
