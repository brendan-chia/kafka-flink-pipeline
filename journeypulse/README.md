# JourneyPulse

JourneyPulse connects food-ordering behavior to customer intelligence using this repository's Kafka, PostgreSQL and monitoring infrastructure.

Phases 1, 2 and 3 are implemented: contracts, isolated setup, an authenticated gateway and SDK, quarantine/metrics, a separate PyFlink profile processor, replay-safe PostgreSQL summaries, onboarding and an authenticated customer API. Segments, targeting and the ordering UI remain in later phases.

## Development phases

| Phase | Guide | Status | Main outcome |
| --- | --- | --- | --- |
| 1 | [Foundation and contracts](../docs/journeypulse/phase-1-foundation.md) | Implemented | Isolated clickstream foundation |
| 2 | [Capture and validation](../docs/journeypulse/phase-2-capture.md) | Implemented | Browser actions reach Kafka through a validated gateway |
| 3 | [Profiles and serving](../docs/journeypulse/phase-3-profiles.md) | Implemented | Separate Flink processor and authenticated profiles |
| 4 | [Segments and targeting](../docs/journeypulse/phase-4-segments.md) | Planned | Behavior and consent determine eligibility |
| 5 | [Demo and analytics](../docs/journeypulse/phase-5-demo.md) | Planned | A visible food-ordering journey and funnel |
| 6 | [Reliability and evidence](../docs/journeypulse/phase-6-reliability.md) | Planned | Measured performance and recovery evidence |

Follow the phases in order. Phase 4 is the first backend MVP; Phase 5 supplies the user-facing demonstration.

## Phase 1 quick start

Run from the repository root. With the existing virtual environment:

~~~powershell
.\.venv\Scripts\python.exe -m pip install -r requirements-journeypulse.txt
.\.venv\Scripts\python.exe scripts/journeypulse.py validate
.\.venv\Scripts\python.exe -m unittest discover -s tests -p 'test_journeypulse_contracts.py' -v
.\.venv\Scripts\python.exe -m unittest discover -s tests -p 'test_journeypulse_foundation.py' -v
~~~

With Docker Desktop running, create the dedicated resources using the existing services:

~~~powershell
docker compose -f docker-compose.yml -f docker-compose.journeypulse.yml up -d kafka postgres
docker compose -f docker-compose.yml -f docker-compose.journeypulse.yml run --rm --build journeypulse-bootstrap
docker compose -f docker-compose.yml -f docker-compose.journeypulse.yml run --rm journeypulse-bootstrap check
~~~

Successful bootstrap/check output has `status: ready`, the two topic names, and five registered event names. Bootstrap exits after setup; it is not a gateway or event producer. It can be rerun without changing matching resources or their registration timestamps.

## Phase 2 quick start

~~~powershell
docker compose -f docker-compose.yml -f docker-compose.journeypulse.yml up -d --build journeypulse-gateway
node scripts/demo_journeypulse_sdk.mjs
~~~

The gateway listens at http://127.0.0.1:8020 with API documentation at /docs and metrics at /metrics. The SDK demo sends five valid actions and an invalid cart, then reports acknowledged outcomes. See the [Phase 2 run/API guide](../docs/journeypulse/phase-2-capture.md) and [SDK guide](sdk/README.md) for session authentication, browser integration, limits and testing.

## Architecture and boundaries

Implemented capture and profile flow:

~~~text
Browser/Node tracking SDK -> event gateway -> journeypulse-events
                                         -> journeypulse-events-quarantine
journeypulse-events -> separate JourneyPulse Flink job -> journeypulse PostgreSQL schema
trusted backend -> customer onboarding/serving API -> attributes and behavioral summaries

JSON examples -> local contract validator

one-shot bootstrap -> Kafka: journeypulse-events
                   -> Kafka: journeypulse-events-quarantine
                   -> PostgreSQL: journeypulse.event_definitions
~~~

Planned complete flow:

~~~text
Food-ordering demo -> tracking SDK -> event gateway -> JourneyPulse Kafka topics
                                                    -> separate Flink job
                                                    -> customer profiles/segments
                                                    -> customer and targeting API
                                                    -> demo/dashboard
~~~

The existing `user-events`, transaction validator, transaction processor, public tables and DataLens API retain their own responsibilities. JourneyPulse does not import or extend the transaction contract. Its clickstream payload has `event_name`, `session_id`, `occurred_at` and event-specific `properties`; monetary fields are not required.

Shared containers mean this is logical separation for local development. It is not a separately secured deployment. Phase 3 supplies optional database capability roles; deployed APIs should use dedicated logins and Kafka permissions.

## Source map

- `contracts/journeypulse/`: versioned JSON Schema and example fixtures.
- `journeypulse/contracts.py`: local validation and per-event registry definitions.
- `journeypulse/config.py`: resource names and environment settings.
- `journeypulse/bootstrap.py`: create/check dedicated resources.
- `journeypulse/sql/001_foundation.sql`: additive foundation migration.
- `scripts/journeypulse.py`: validate/bootstrap/check CLI.
- `docker-compose.journeypulse.yml`: optional setup overlay.
- `tests/test_journeypulse*.py`: offline foundation checks.
- `.github/workflows/journeypulse.yml`: offline and live infrastructure checks.

Phase 2 source additions: `journeypulse/gateway.py`, `auth.py`, `publisher.py`,
`journeypulse/sdk/`, `scripts/journeypulse_gateway.py`, `scripts/demo_journeypulse_sdk.mjs`
and `tests/check_journeypulse_capture.py`. Gateway dependencies live in
`requirements-journeypulse-gateway.txt` so foundation setup remains small.

## Phase 3 quick start

Rerun bootstrap to apply the additive profile migration on existing volumes:

```powershell
docker compose -f docker-compose.yml -f docker-compose.journeypulse.yml run --rm --build journeypulse-bootstrap
docker compose -f docker-compose.yml -f docker-compose.journeypulse.yml up -d --build journeypulse-gateway journeypulse-customers journeypulse-processor
.\.venv\Scripts\python.exe tests/check_journeypulse_profiles_db.py
.\.venv\Scripts\python.exe tests/check_journeypulse_profiles.py --restart
```

The customer API serves 8021; the dedicated Flink UI uses 8082. A trusted backend obtains customer-scoped tokens from the customer API's `POST /v1/sessions`. Onboard with `POST /v1/customers`, read `GET /v1/customers/{user_id}`, and update language/explicit consent with `PATCH /v1/customers/{user_id}`. Tracking tokens cannot authorize these operations. The [Phase 3 guide](../docs/journeypulse/phase-3-profiles.md) provides runnable examples, migrations, optional database roles and recovery instructions.

Pre-onboarding events create a placeholder with consent false. Behavior never overwrites attributes. Durable transactional Kafka receipts protect checkpoint replay; event IDs deduplicate new deliveries for seven days from processing, and permanent order identities protect order totals. After the event-ID window, a new-offset notification can count again. Receipts/orders grow indefinitely and require consistent backups. Freshness describes summary age, not consumer lag. Phase 4 segments are not implemented.

Phase 3 sources: `profiles.py`, `processor.py`, `customer_api.py`, `sql/002_profiles.sql`, optional `sql/roles.sql`, two new launch scripts/Dockerfiles, `requirements-journeypulse-processor.txt`, offline profile tests and `tests/check_journeypulse_profiles*.py`. Offline checks, actual Flink graph construction and real PostgreSQL tests passed locally using an isolated temporary PostgreSQL 18 instance. Full Kafka/Flink integration remains unverified locally because Docker's engine and Kafka were unavailable; executable probes and CI cover it.
