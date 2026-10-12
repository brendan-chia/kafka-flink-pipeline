# Phase 5 â€” Demo app and journey analytics

Status: planned. Dependency: Phase 4 returns profiles and targeting eligibility.

## Objective

Let a person perform a food-ordering journey and inspect its effect on customer intelligence.

## Planned implementation

1. Build a small React demo with restaurant browsing, cart, checkout and simulated order completion.
2. Instrument the five registered event types through the SDK; preserve session, cart and order identities.
3. Provide an explicit consent/onboarding control and show the current demo customer's profile and eligibility.
4. Add session-based funnel aggregates for session start, restaurant view, add to cart, checkout start and order completion.
5. Add a focused dashboard for throughput, validation failures, processing freshness, segments and funnel conversion.
6. Keep operational infrastructure metrics in Grafana; link the customer experience to those views where useful.
7. Add browser instrumentation checks against emitted payloads and contracts.

The ordering UI simulates commerce; it does not require a real payment gateway or notification delivery.

## Analytics decisions

Define funnel attribution before writing queries:

- The counting unit is a session, not the number of event deliveries.
- A converted session must contain the required stages in the defined chronological order.
- Multiple carts/orders in one session and the attribution window need an explicit policy.
- Delayed events may revise completed-session analytics according to the lateness policy.
- Explain denominator, reporting interval and freshness next to conversion results.

Show pending/stale data states while the backend catches up. A zero is different from an unavailable query.

## Verification

- UI actions emit the correct names and required properties.
- A complete browser journey appears in the profile and funnel.
- Abandonment changes the displayed segment and eligibility without another click.
- Completing the matching cart clears abandonment.
- Consent changes are reflected by targeting.
- Duplicate delivery does not inflate funnel counts.
- Invalid events and backend outages are visible in understandable states.

## Deliverables and exit criteria

Deliver the demo, dashboard, instrumentation checks and one documented start command. A reviewer must be able to follow an action from browser to ingestion, processing, profile and eligibility.

Next: [Phase 6 â€” Reliability and portfolio evidence](phase-6-reliability.md).
