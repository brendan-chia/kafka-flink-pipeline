# Phase 4 - Segments and targeting

Status: implemented. This completes the first backend MVP. Phase 5's ordering UI
and actual reminder delivery are not included.

## Version 1 semantics

| Segment | Rule | Exit/update |
| --- | --- | --- |
| `active_browser` | At least five accepted restaurant views in `(evaluation_time - 600 seconds, evaluation_time]` | Expires when fewer than five remain; restaurants need not be distinct |
| `abandoned_cart` | At least one unresolved customer/cart whose latest add occurrence plus 600 seconds is at or before evaluation time | Matching order completion permanently resolves that cart |
| `repeat_customer` | At least three durable distinct `(user_id, order_id)` completions | Maintained from Phase 3 permanent order identities |

Membership describes behavior independently of consent. Per-segment eligibility
requires matching behavior, current evaluation, onboarding and explicit current
marketing consent. Overall eligibility is the OR of segment eligibility.

Carts use `(user_id, cart_id)`. Completing A never resolves B. Completed carts are
terminal even when completion arrives before an add or an add replays after the
seven-day event-ID horizon. Use a new cart ID for a new shopping journey.
The maximum accepted add occurrence defines the deadline: a delayed older add
cannot move it backwards; a delayed newer add can postpone it and remove
provisional abandonment. An old first add can abandon immediately on processing.
`checkout_started` records its latest occurrence but neither postpones the
deadline nor resolves the cart, and checkout alone does not create abandonment.

Delayed completion corrects membership in the same transaction as its accepted
notification. Existing order facts remain immutable: another notification for an
existing order uses its first accepted cart and cannot resolve a different cart.
Duplicate notifications, new offsets and checkpoint replay cannot inflate
distinct order totals.

There is no event-time lateness cutoff for valid behavior. Views outside the
current lookback do not enter active-browser membership. Future views cannot
enter before occurrence; gateway future-skew checks remain unchanged.
Results are provisional until delayed events arrive.

## Scheduling, silence and downtime

The existing PyFlink writer persists views, cart changes and all three memberships
inside the Phase 3 receipt/event/order/summary transaction, preserving consumer
group, operator UID and checkpoint design. Duplicate/conflict/invalid/replay
receipts cannot repeat effects. Segment failures roll back the entire record and
fail the task.

A separate `journeypulse-segments` worker evaluates durable due rows using the
PostgreSQL UTC wall clock. It does not depend on event-time watermarks or new
customer traffic. By default it polls every five seconds, locks the same
behavioral rows as the processor, and evaluates up to 100 customers per
transaction using `FOR UPDATE SKIP LOCKED`. Full batches drain with a 50 ms pause.
Customers with behavior are periodically reevaluated, including inactive users.
An onboarded customer with no processed behavior remains unevaluated.

`segment_evaluations` persists version, evaluation time and next due time.
The next due time is the earliest of the next poll, view expiry, future view
occurrence and unresolved cart deadline. Missed deadlines survive downtime.
Startup evaluates overdue state at the current database time: old browser
membership expires and overdue unresolved carts abandon without new input.
Multiple workers can share the queue with identical settings.

Normal transition delay is one poll plus database processing time, not a measured
SLO. Backlogs, locks and outages can extend it. Targeting also checks active
membership's `valid_until`, so an expired browser cannot remain eligible while
the worker catches up. Evaluation older than 30 seconds fails eligibility closed.
Fresh evaluation does not establish that Kafka/Flink has no backlog or that all
completion events have arrived.

| Variable (prefix every name with `JOURNEYPULSE_SEGMENT_`) | Default | Meaning |
| --- | --- | --- |
| `WINDOW_SECONDS` | 600 | Active-browser lookback |
| `ABANDON_SECONDS` | 600 | Delay after maximum add occurrence |
| `POLL_SECONDS` | 5 | Scheduler cadence |
| `STALE_SECONDS` | 30 | Eligible evaluation age limit; at least twice poll |
| `BATCH_SIZE` | 100 | Maximum customers per scheduler transaction |
| `METRICS_PORT` | 8022 | Host worker metric listener |

Values must be positive integers. Short thresholds are supported for tests.
Default behavior definitions have rule version `1`; other behavior thresholds
derive a stable `1-<definition hash>`. Poll/freshness/batch changes do not change
the behavior version. Bootstrap registers immutable JSON rules and rejects
altered definitions. Configure bootstrap, every Flink process, worker, API and
scenario identically; rerun bootstrap before changing behavior thresholds.
This is not hot rule management. A longer view window cannot recover timestamps
already pruned under a shorter window.

The JourneyPulse Flink image now copies Python 3.11 into the Java 17 Flink base
instead of using that base's Python 3.12. This corrects a Phase 3 runtime
prerequisite to match repository guidance. Optional `JOURNEYPULSE_FLINK_IMAGE`
selects an image tag for isolated deployments; the default remains
`journeypulse-pyflink:2.2.1`.
Live verification also found a Phase 3 startup bug: the processor called singular
`get_metric_group()` instead of PyFlink 2.2.1's `get_metrics_group()`. The corrected
call preserves all five existing outcome counters and lets the writer start.

## Upgrade and run

Pause the dedicated JourneyPulse processor and worker before upgrading.
Bootstrap applies `001`, `002`, then additive `003_segments.sql` in the existing
serialized transaction. Fresh installations follow the same path. Repeated
bootstrap/check preserves registration/migration timestamps.
Apply updated optional `journeypulse/sql/roles.sql` after migration 003 when
using capability logins. The worker uses the processor login; serving cannot
write memberships.

~~~powershell
docker compose -f docker-compose.yml -f docker-compose.journeypulse.yml config --quiet
docker compose -f docker-compose.yml -f docker-compose.journeypulse.yml up -d kafka postgres
docker compose -f docker-compose.yml -f docker-compose.journeypulse.yml run --rm --build journeypulse-bootstrap
docker compose -f docker-compose.yml -f docker-compose.journeypulse.yml run --rm journeypulse-bootstrap check
docker compose -f docker-compose.yml -f docker-compose.journeypulse.yml up -d --build journeypulse-gateway journeypulse-customers journeypulse-processor journeypulse-segments
~~~

Existing summaries/orders remain intact. Migration 003 backfills terminal carts
from completed orders. Phase 3 retained neither individual view timestamps nor
open-cart add history; receipt replay deliberately does not repeat effects.
Therefore rolling views/open carts start with newly accepted Phase 4 events.
Existing distinct-order history supports repeat-customer membership on the
first worker pass. No transaction or DataLens schema is changed.

Host worker: `python -m journeypulse.segment_worker`, using pinned gateway
dependencies, including the existing Prometheus client. Customer API remains
on 8021, dedicated Flink REST on 8082, worker metrics on 8022.

## Targeting API

`GET /v1/customers/{user_id}/targeting` preserves Phase 3's customer audience,
path identity, origin checks and rate budget. Tracking tokens cannot read it.
Consent and membership share a read-only repeatable snapshot. The customer API
retains attribute/consent ownership. A successful revocation affects the next
response without a clickstream event or worker tick. Responses are `no-store`.

Obtain `$headers` using the [Phase 3 token/onboarding example](phase-3-profiles.md):

~~~powershell
Invoke-RestMethod -Uri http://127.0.0.1:8021/v1/customers/u123/targeting -Headers $headers
$revoke = @{ marketing_consent=$false } | ConvertTo-Json
Invoke-RestMethod -Method Patch -Uri http://127.0.0.1:8021/v1/customers/u123 -ContentType application/json -Headers $headers -Body $revoke
Invoke-RestMethod -Uri http://127.0.0.1:8021/v1/customers/u123/targeting -Headers $headers
~~~

Overall fields: `eligible`, `marketing_consent`, `consent_status`
(`granted` or `missing_or_revoked`), `consent_updated_at`, `rule_version`,
`evaluated_at`, `served_at`, `freshness` and all three `segments`.
Each segment has `member`, `eligible`, `reasons`, `behavior_reason`,
numeric `evidence`, version, evaluation time and `valid_until`.
Evidence counts describe the stored evaluation, not a new count on each read.
An active membership expiring before reevaluation reports
`active_browser_window_expired` and is immediately ineligible.
For example, an abandoned segment contains:

~~~json
{
  "member": true,
  "eligible": true,
  "reasons": ["uncompleted_carts_past_deadline"],
  "behavior_reason": "uncompleted_carts_past_deadline",
  "evidence": {"count": 1},
  "rule_version": "1",
  "evaluated_at": "2026-10-12T08:10:00Z",
  "valid_until": null
}
~~~

`freshness.status` is `current`, `stale` or `unevaluated`, with
`evaluation_age_seconds`, `stale_after_seconds`, `next_evaluation_at`,
`behavior_processed_at` and scope `segment_evaluation_age_not_consumer_lag`.
Stale stored membership is evidence only; it cannot confer eligibility.
Missing evaluation, partial memberships or a rule mismatch yields null members
and `behavior_unevaluated`. No match gives `behavior_not_matched`; missing or
revoked consent gives `marketing_consent_required`. Counts and behavior reasons
explain the rule result. Placeholders default to consent false. Attributes never
fabricate behavioral freshness.

401/403/429 retain Phase 3 semantics. Unknown customer: 404. Database outage,
missing tables or query failure: sanitized 503 `database_unavailable`.
Readiness now requires migration 3. Unavailable data cannot produce a successful
eligible result.

## Metrics and verification

Worker metrics expose collection health, last successful cycle timestamp,
evaluation/failure counters, oldest overdue seconds and membership counts labeled
only by the three segment names. Membership gauges describe persisted behavior
independently of consent; use health, last success and overdue alongside them.
API `/metrics` counts authorized reads by fixed results `current`, `stale`,
`unevaluated`, `absent`, `unavailable`. No customer/cart/event IDs or raw
errors are labels. Optional Prometheus scrapes are included; their targets are
down when JourneyPulse is disabled. These metrics do not measure consumer lag
or unique eligible customers.

~~~powershell
.\.venv\Scripts\python.exe -m unittest discover -s tests -p 'test_journeypulse*.py' -v
node --test journeypulse/sdk/journeypulse.test.mjs
.\.venv\Scripts\python.exe scripts/journeypulse.py validate
.\.venv\Scripts\python.exe tests/check_journeypulse_plan.py
.\.venv\Scripts\python.exe tests/check_journeypulse_profiles_db.py
.\.venv\Scripts\python.exe tests/check_journeypulse_segments_db.py
.\.venv\Scripts\python.exe scripts/demo_journeypulse_targeting.py
~~~

Offline tests cover thresholds, exact expiry boundaries, future/late events,
separate carts, delayed completion, distinct orders, consent, stale/unevaluated
state, authorization and durable scheduling.
The PostgreSQL probe uses unique customers, synthetic partition-98 receipts,
short thresholds and an injected evaluator clock. It verifies transactions,
duplicates, replay from a new store, immutable order/cart facts, concurrency,
downtime catch-up and rollback after membership insertion.
It also verifies Phase 3 order/cart backfill and repeated migration application.
Set `JOURNEYPULSE_CHECK_ROLES=1` after applying roles for capability checks.
Test evidence is retained.

The HTTP scenario sends add-cart through the gateway, waits through input
silence, retrieves abandonment eligibility, revokes/restores consent, duplicates
the add, sends delayed completion and verifies correction. It requires the
running Kafka/Flink/API/worker stack and waits ten minutes by default.
For a short scenario set these **before bootstrap and service startup**:

~~~powershell
$env:JOURNEYPULSE_SEGMENT_ABANDON_SECONDS='5'
$env:JOURNEYPULSE_SEGMENT_WINDOW_SECONDS='30'
$env:JOURNEYPULSE_SEGMENT_POLL_SECONDS='1'
.\.venv\Scripts\python.exe scripts/demo_journeypulse_targeting.py --restart-worker
~~~

`--restart-worker` stops only the dedicated Compose worker across the deadline,
then restarts it; use on the dedicated local stack.
CI runs the scenario, both database probes with roles, repeated migrations,
the actual Flink graph, and Phase 3's `--restart` Kafka/Flink replay probe.
Clock-controlled DB tests and graph construction alone are not proof of live
transport or checkpoint recovery.

## Limits

Views are pruned after leaving the lookback. Resolved carts, orders, receipts
and versioned membership metadata stay durable and grow over time.
There is no reminder dispatch, historical evaluation audit log, cart reopening,
bulk rule management, production SLO, consumer-lag gate or repair of lost Kafka
history. Keep consistent backups of the entire JourneyPulse ledger.
Phase 3's seven-day event-ID horizon and permanent receipt/order guarantees
remain in effect. Per-customer queries and synchronous per-event transactions
need measured capacity work for large histories. Source IDs share globally
unique customer IDs as in Phase 3.

Next: [Phase 5 - Demo and analytics](phase-5-demo.md).
