# Phase 2 - Capture and validation

Status: implemented. Dependency: Phase 1 foundation setup has completed.

The standalone FastAPI gateway and browser SDK capture clickstream behavior. Customer processing and serving are implemented in Phase 3; segmentation remains in Phase 4.

## Implemented components

- A gateway on loopback port 8020, with server-side source registrations and signed 15-minute browser/session tokens.
- `POST /v1/events` for validated, bounded batches with individual acknowledged/rejected/retryable outcomes.
- Kafka publication keyed by the verified user, with trusted source and receipt metadata wrapped around the frozen event payload.
- Raw-byte quarantine for authenticated malformed requests, invalid batches and rejected events.
- A JavaScript SDK with bounded buffering, stable IDs, partial retries, backoff, token refresh and best-effort unload delivery.
- Source-specific origins, source request budgets, in-flight/body/time limits and metrics.
- Compose gateway service, API/SDK tests, command-line/browser examples and a live SDK/HTTP/Kafka integration probe.

## Start with Docker

Docker Desktop's Linux engine must be running. From the repository root:

~~~powershell
docker compose -f docker-compose.yml -f docker-compose.journeypulse.yml config --quiet
docker compose -f docker-compose.yml -f docker-compose.journeypulse.yml up -d --build journeypulse-gateway
docker compose -f docker-compose.yml -f docker-compose.journeypulse.yml logs journeypulse-bootstrap journeypulse-gateway
~~~

The gateway depends on successful foundation bootstrap. This creates/verifies the two topics and catalogue on existing or fresh PostgreSQL volumes. It starts only the required Kafka/PostgreSQL dependencies, not the existing transaction Flink job or DataLens API.

Open `http://127.0.0.1:8020/docs`. Check `/health/live` and `/v1/health/ready`. Liveness indicates a running process; readiness actively requests broker metadata and requires both three-partition topics with leaders. Readiness is a point-in-time check, not a promise that a subsequent publish succeeds.

The healthcheck uses liveness so broker outages do not restart an otherwise functioning gateway. Ingestion returns retryable outcomes during those outages.

## Start on the host

~~~powershell
.\.venv\Scripts\python.exe -m pip install -r requirements-journeypulse-gateway.txt
.\.venv\Scripts\python.exe scripts/journeypulse_gateway.py
~~~

The host gateway uses `JOURNEYPULSE_KAFKA_BOOTSTRAP`, default `localhost:29092`. Compose uses `kafka:9092`. The gateway does not access PostgreSQL at request time; its frozen versioned contracts are loaded from the packaged schema. Bootstrap provides the catalogue. `GET /v1/contracts` exposes those definitions to authenticated clients.

## Source registration and session issuance

Sources are registered in configuration, not through a public registration API.

The default local source is `demo-app`, with source key `journeypulse-local-source-key`. Its browser origins are `http://localhost:5173` and `http://127.0.0.1:5173`. The default local signing key is `journeypulse-local-signing-key-change-this`.

Override the single demo source key and signing key with `JOURNEYPULSE_SOURCE_KEY` and `JOURNEYPULSE_SIGNING_KEY`. Multiple sources use `JOURNEYPULSE_SOURCES_JSON`, a JSON array of objects with `source_id`, `server_key` and `origins`. Keys must have at least 16 bytes for a source and 32 bytes for signing. Origins must be explicit HTTP(S) origins with no wildcard or path.

Sources can be updated by restarting the gateway with new configuration. Removing a source or rotating the signing key invalidates its tokens. Rotating only a source key prevents old credentials from issuing new tokens; previously issued tokens remain valid until expiry.

A trusted application backend calls:

~~~http
POST /v1/sessions
X-Source-Key: <server-side source key>
Content-Type: application/json

{"source_id":"demo-app","user_id":"u123","session_id":"s456"}
~~~

The backend must derive user/session identity from its authenticated application context. It must not forward arbitrary identity values supplied by an untrusted browser.

Runnable PowerShell example for local development:

~~~powershell
$sessionRequest = @{ source_id = 'demo-app'; user_id = 'u123'; session_id = 's456' } | ConvertTo-Json
$sourceKey = if ($env:JOURNEYPULSE_SOURCE_KEY) { $env:JOURNEYPULSE_SOURCE_KEY } else { 'journeypulse-local-source-key' }
$trackingSession = Invoke-RestMethod -Method Post -Uri http://127.0.0.1:8020/v1/sessions -ContentType application/json -Headers @{ 'X-Source-Key' = $sourceKey } -Body $sessionRequest
~~~

The result contains `access_token`, `expires_at`, `expires_in`, `user_id`, `session_id` and `source_id`. Tokens are HMAC-signed credentials specific to this gateway, not general-purpose JWTs. Token/session responses use `Cache-Control: no-store`.

Browser requests to session issuance are rejected when they contain an Origin header. The browser obtains its token from the application backend and sends `Authorization: Bearer <access_token>` to the ingestion API. Each event's user/session must match the token. Source keys and signing keys are server-only.

This phase provides tracking authentication, not an end-user login system or a marketing-consent decision.

## Event ingestion

~~~http
POST /v1/events
Authorization: Bearer <access_token>
Content-Type: application/json

{"events":[{"event_id":"evt_001","event_name":"session_started","schema_version":1,"user_id":"u123","session_id":"s456","occurred_at":"2026-10-12T08:00:00Z","properties":{"platform":"web"}}]}
~~~

Use a current occurrence time for new actions. Delayed historical events are accepted; more than 60 seconds of future skew is rejected.

Every result has `index`, `event_id` where valid, `status` and `errors`. Accepted results include Kafka `delivery` topic/partition/offset. Rejected results include `quarantine_persisted`. Retryable results indicate uncertain/unacknowledged delivery; resend the unchanged event.

| HTTP status | Meaning |
| --- | --- |
| 200 | Every event has a Kafka acknowledgement |
| 207 | Mixed/permanent event outcomes; inspect every result |
| 503 with results | Every valid event in the batch is unacknowledged/retryable |
| 400 | Malformed strict UTF-8 JSON, with acknowledged quarantine |
| 422 | Invalid batch envelope or batch count, with acknowledged quarantine |
| 503 with detail | Request-level quarantine could not be acknowledged |
| 401 | Missing/invalid/expired token or invalid source credentials |
| 403 | Disallowed source origin or browser session issuance |
| 413 | Body exceeds the byte cap; no quarantine of oversized bodies |
| 415 | Content type is not JSON or compression is unsupported |
| 408 | Incomplete/slow body exceeded its read deadline |
| 429 | Source request budget or ingestion capacity exceeded |

HTTP 207 also covers an entirely invalid batch of individual events. Schema failures are permanent even if quarantine storage is unavailable: the event remains rejected and `quarantine_persisted: false` makes evidence loss/uncertainty visible. Do not resend invalid events unchanged. Fix them and use a new ID for a corrected action.

Request-level malformed/batch errors use a different response shape (`detail`) because there is no valid event list to acknowledge. Authentication, origin, body-size and content-type failures do not publish records.

Error arrays are capped at 16 entries, messages at 256 characters and field paths at 200 characters. Quarantine preserves the raw request for fuller debugging.

## Transport and quarantine formats

Accepted Kafka value:

~~~json
{
  "transport_version": 1,
  "request_id": "generated-request-uuid",
  "source_id": "demo-app",
  "received_at": "2026-10-12T08:00:00Z",
  "event": {
    "event_id": "evt_001",
    "event_name": "session_started",
    "schema_version": 1,
    "user_id": "u123",
    "session_id": "s456",
    "occurred_at": "2026-10-12T08:00:00Z",
    "properties": {"platform": "web"}
  }
}
~~~

The key is the authenticated user ID. A future processor must read `event` for customer behavior and keep `received_at` separate from occurrence time. The transport wrapper is not submitted as part of the v1 clickstream event.

A quarantine record contains transport/request/source/user/session metadata, `batch_index`, `event_id` where valid, bounded errors, a bounded rejection reason and `raw_request_base64`. Base64 preserves the exact request bytes, including whitespace or invalid UTF-8. For invalid entries in a batch, the full batch request is retained once per rejected entry so its exact original representation remains available. Quarantine therefore can include valid neighboring events; treat it as customer data. Topic retention remains seven days.

Oversized or incomplete requests are deliberately not persisted as partial/oversized raw records.

## Limits and delivery behavior

| Gateway limit | Value |
| --- | --- |
| Events per request | 1â€“50 |
| HTTP body | 256 KiB, checked for declared and streamed sizes |
| Future occurrence skew | 60 seconds |
| Session lifetime | 900 seconds |
| Request body/overall ingestion budget | 10 seconds |
| Kafka publication budget per batch | Up to 5 seconds, within remaining request budget |
| In-flight event requests | 16 per process |
| Source request budget | 120 requests/minute, shared by issuance and ingestion |
| Kafka send metadata/buffer wait | 200 ms per enqueue |
| Kafka client request timeout | 3 seconds |
| Producer retry count | 3, with one in-flight Kafka request per connection |

The event endpoint measures its budget from body reading through publication. Serialization, response handling and scheduling add overhead; this is a bounded application workflow rather than a proxy-enforced hard deadline. The SDK's default timeout is 15 seconds.

Rate budgets and counters are process-local. Run the supplied single-process gateway for this version. Multiple replicas would need coordinated rate limiting and metrics aggregation.

The producer uses `acks=all`. On this one-broker local setup, that means the single replica's acknowledgement; it does not demonstrate replicated durability. It uses the Kafka 2.6 protocol supported by the existing broker image.

A queued record is never returned as accepted. An acknowledgement timeout or a lost HTTP response can still leave a delivered record in Kafka. SDK retries preserve IDs; Phase 3 deduplicates new deliveries within its documented retention window and protects checkpoint replay with durable receipts. There is no universal end-to-end exactly-once claim.

Quarantine and accepted events are independently acknowledged. Kafka and the HTTP response are not one atomic transaction. Interrupted calls can leave some records delivered and others uncertain.

## SDK and runnable demonstrations

See the [SDK guide](../../journeypulse/sdk/README.md) for imports, token providers, buffer/retry limits, callbacks and a browser-console probe.

Two finite command-line demonstrations are available:

~~~powershell
.\.venv\Scripts\python.exe scripts/demo_journeypulse_capture.py
node scripts/demo_journeypulse_sdk.mjs
~~~

Both send five valid events and an invalid cart. Expect five accepted, one rejected, zero retryable/dropped, and an acknowledged quarantine entry. These are server-side scripts: their source key comes from the environment or the local development default. The SDK demo uses the same module shipped for browser instrumentation.

Use `JOURNEYPULSE_GATEWAY_URL` and `JOURNEYPULSE_SOURCE_ID` to configure the Node demo. The Python demo accepts `--url` and `--source`.

## Metrics

The gateway exposes `/metrics`:

- `journeypulse_gateway_events_total{status}`: accepted/rejected/retryable event outcomes.
- `journeypulse_gateway_rejections_total{reason}`: fixed rejection categories.
- `journeypulse_gateway_kafka_failures_total{channel}`: unacknowledged event/quarantine attempts.
- `journeypulse_gateway_request_seconds`: response latency histogram by route template and status.

There are no user/session/event-ID metric labels. Request-level bad JSON contributes to rejection/Kafka counters, not event-delivery counts. The request-latency histogram includes successful and failed responses; use the status label when calculating successful ingestion latency.

To scrape from the existing Compose network, add a Prometheus scrape job targeting `journeypulse-gateway:8020`. The base monitoring configuration is unchanged.

## Verification

Offline:

~~~powershell
.\.venv\Scripts\python.exe -m pip install -r requirements-journeypulse-gateway.txt httpx==0.28.1
.\.venv\Scripts\python.exe -m unittest discover -s tests -p 'test_journeypulse*.py' -v
node --test journeypulse/sdk/journeypulse.test.mjs
~~~

Live after starting the gateway:

~~~powershell
.\.venv\Scripts\python.exe tests/check_journeypulse_capture.py
~~~

The live probe waits for readiness, invokes the real SDK over HTTP, consumes both Kafka topics and verifies the user key, payload contracts, duplicate delivery, malformed raw bytes and identity rejection. It publishes a uniquely identified test journey. It requires Node 22+ and the host Kafka endpoint.

The [JourneyPulse CI workflow](../../.github/workflows/journeypulse.yml) runs offline tests, repeated foundation setup and the live probe against actual containers. No live throughput or customer-profile freshness claims are made in this phase.

## Completion checks

- Five event types can be submitted through the SDK and acknowledged in the correct topic.
- Invalid carts return actionable paths and quarantine records.
- Wrong source/user/session/origin, malformed payloads, unsupported versions and limits have documented outcomes.
- Kafka outage produces retryable delivery rather than false acceptance.
- Partial retries preserve identities and exclude accepted/rejected entries.
- The gateway can run/restart independently of existing transaction and DataLens services.

Next: [Phase 3 - Customer profiles and serving](phase-3-profiles.md).
