# Phase 1 â€” Foundation and contracts

Status: implemented. This phase establishes a separate clickstream module without changing the existing transaction ingestion, Flink job, DataLens API or base Compose file.

## Deliverables

- Five version-1 event definitions in [the JSON Schema](../../contracts/journeypulse/events-v1.schema.json).
- A complete [valid journey](../../contracts/journeypulse/examples/valid-journey.json) and an [invalid cart event](../../contracts/journeypulse/examples/invalid-add-to-cart.json).
- Local validation with structured field paths and all detected rule failures.
- Two dedicated topics, each with three partitions, one replica, seven-day retention, deletion cleanup and broker append timestamps.
- The `journeypulse` PostgreSQL schema, `event_definitions` catalogue and `schema_migrations` marker.
- A repeatable one-shot setup command, read-only readiness check, optional Compose overlay and CI integration workflow.

No events are published during setup. Customer tables belong to Phase 3.

## Contract

Every event requires `event_id`, `event_name`, `schema_version`, `user_id`, `session_id`, `occurred_at` and `properties`.

| Event | Required properties |
| --- | --- |
| session_started | platform: web, ios or android |
| restaurant_view | restaurant_id |
| add_to_cart | cart_id, restaurant_id, item_id, quantity |
| checkout_started | cart_id |
| order_completed | order_id, cart_id, restaurant_id |

Identifiers use 1â€“100 ASCII letters, digits or `_.:-`, starting with a letter or digit. Quantity is an integer from 1 to 1000. Date/time validation is enabled: `occurred_at` requires an RFC 3339 timestamp with a timezone; offsets are accepted and processors will normalize to UTC. Unknown envelope fields and event properties are rejected. Schema version must be the integer value 1.

The schema uses [JSON Schema Draft 2020-12](https://json-schema.org/draft/2020-12). Format checking is explicitly enabled; a schema's `format` keyword alone does not ensure timestamp validation.

Retries must preserve the event ID and original payload. Reusing an ID for a different action is a contract violation. The gateway will assign `received_at` separately in Phase 2; the browser does not supply trusted receipt time.

## Local validation

Run from the repository root:

~~~powershell
.\.venv\Scripts\python.exe -m pip install -r requirements-journeypulse.txt
.\.venv\Scripts\python.exe scripts/journeypulse.py validate
.\.venv\Scripts\python.exe scripts/journeypulse.py validate contracts/journeypulse/examples/invalid-add-to-cart.json
.\.venv\Scripts\python.exe -m unittest discover -s tests -p 'test_journeypulse_contracts.py' -v
.\.venv\Scripts\python.exe -m unittest discover -s tests -p 'test_journeypulse_foundation.py' -v
~~~

The default fixture passes with exit code 0. The invalid fixture intentionally exits 1 and reports `$.properties` for missing `item_id` and `$.properties.quantity` for the invalid quantity. Duplicate JSON keys, NaN, Infinity and malformed JSON are rejected before schema validation.

This phase validates shape and values. Payload-size limits, client clock skew, user authentication and HTTP error responses belong to the gateway.

## Infrastructure setup

Prerequisite: Docker Desktop's Linux engine must be running.

~~~powershell
docker compose -f docker-compose.yml -f docker-compose.journeypulse.yml config --quiet
docker compose -f docker-compose.yml -f docker-compose.journeypulse.yml up -d kafka postgres
docker compose -f docker-compose.yml -f docker-compose.journeypulse.yml run --rm --build journeypulse-bootstrap
docker compose -f docker-compose.yml -f docker-compose.journeypulse.yml run --rm journeypulse-bootstrap
docker compose -f docker-compose.yml -f docker-compose.journeypulse.yml run --rm journeypulse-bootstrap check
~~~

The repeated bootstrap demonstrates setup idempotency. The check performs read-only catalogue and topic verification. The setup retries initial client connections for up to 60 seconds per service, with bounded client connection/request timeouts. If a broker is restarting during an administrative operation, rerun bootstrap.

For an existing database, bootstrap applies the migration explicitly; it does not depend on PostgreSQL's fresh-volume initialization hooks. No database reset is needed.

Resources:

| Resource | Name |
| --- | --- |
| Accepted clickstream topic | journeypulse-events |
| Invalid-event quarantine topic | journeypulse-events-quarantine |
| Contract definitions | journeypulse.event_definitions |
| Module migration marker | journeypulse.schema_migrations, version 1 |

Inspect them:

~~~powershell
docker exec kafka kafka-topics --bootstrap-server kafka:9092 --describe --topic journeypulse-events
docker exec kafka kafka-topics --bootstrap-server kafka:9092 --describe --topic journeypulse-events-quarantine
docker exec postgres psql -U grabuser -d grabevents -c "SELECT event_name, schema_version FROM journeypulse.event_definitions ORDER BY event_name;"
~~~

The catalogue must have the five v1 rows. Each stored schema is self-contained and restricted to its event name.

## Host configuration

You can also run `python scripts/journeypulse.py bootstrap` or `check` from the host.

| Environment variable | Host default | Compose setup |
| --- | --- | --- |
| JOURNEYPULSE_KAFKA_BOOTSTRAP | localhost:29092 | kafka:9092 |
| JOURNEYPULSE_POSTGRES_HOST | localhost | postgres |
| JOURNEYPULSE_POSTGRES_PORT | 5432 | 5432 |
| JOURNEYPULSE_POSTGRES_DB | grabevents | environment/default |
| JOURNEYPULSE_POSTGRES_USER | grabuser | environment/default |
| JOURNEYPULSE_POSTGRES_PASSWORD | grabpass | environment/default |

These are existing local-development credentials. A custom database must already exist. The optional overlay fixes container network endpoints; host endpoint overrides apply to the host CLI. Topic names and topology are deliberately fixed for this initial local version.

## Compatibility and failure behavior

Bootstrap creates missing topics and verifies existing topics; it never alters or deletes them. Wrong partitions, replication, retention, cleanup or timestamp configuration cause failure so the discrepancy can be reviewed.

Registered `(event_name, schema_version)` definitions are immutable through bootstrap. Matching rows are preserved; a changed v1 definition fails and the database transaction rolls back. Breaking changes require a new schema version and rollout design. This is setup-level immutability, not a database permission policy for arbitrary writers.

The database migration and registrations are one transaction, serialized by an advisory lock. Kafka creation and PostgreSQL setup cannot share a transaction. If database setup fails after topic creation, the topics remain and rerunning bootstrap completes the missing work.

Do not feed clickstream events into `user-events`: the existing transaction validator requires monetary fields and different event names.

## Acceptance and verification

- All five examples pass; invalid properties, identifiers, timestamps and versions fail.
- Per-event registry definitions reject a different event name.
- Existing transaction contract tests still pass.
- New topic creation uses only dedicated names; matching existing resources are only inspected.
- Conflicting catalogue definitions fail rather than being overwritten.
- Bootstrap succeeds twice and read-only check reports ready against real Kafka/PostgreSQL.
- Existing public and DataLens tables remain accessible.

The offline tests cover contract and setup decisions. The dedicated [CI workflow](../../.github/workflows/journeypulse.yml) exercises the real Docker services, repeated bootstrap and the read-only check. Run that workflow or the commands above to obtain live evidence.

Next: [Phase 2 â€” Capture and validation](phase-2-capture.md).
