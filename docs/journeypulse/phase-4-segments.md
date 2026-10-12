# Phase 4 â€” Segments and targeting

Status: planned. Dependency: Phase 3 produces replay-safe customer profiles.

## Objective

Translate customer behavior into explainable segments and consent-aware targeting eligibility.

## Initial rules

| Segment | Proposed rule | Exit/update behavior |
| --- | --- | --- |
| Active browser | At least five restaurant-view events in the trailing ten minutes | Remove when the rolling count falls below five |
| Abandoned cart | Cart activity has not led to a completed order after ten minutes | Remove on completion of the corresponding cart |
| Repeat customer | At least three distinct completed orders | Maintain from deduplicated order history |

Five view events is the initial interpretation; distinct restaurants would be a different rule. Freeze the final definitions and rule versions before implementation.

## Planned implementation

1. Add segment membership, rule version, eligibility reason and evaluated-time storage.
2. Implement rolling behavior and timer-driven cart detection in the JourneyPulse processor.
3. Track carts separately; completion of cart A must not resolve cart B.
4. Handle delayed checkout/order events and replay without resurrecting resolved carts.
5. Add `GET /v1/customers/{user_id}/targeting` with eligibility, reasons, consent status, rule version and freshness.
6. Add membership/targeting metrics without per-user metric labels.

Eligibility requires explicit marketing consent and applicable current behavior. Revoked consent must affect the next targeting response; it must not wait for another clickstream event.

## Time and recovery decisions

The system must transition an idle cart without another customer event. Event-time timers alone can stall when all input becomes idle, so explicitly choose and test a scheduling policy: processing-time evaluation, a clock/heartbeat strategy, or another bounded evaluation mechanism.

Document allowed lateness and whether segment results are provisional. A delayed completed order must correct abandonment. Persist enough state to distinguish resolved carts during replay. Define how active-browser memberships expire during input silence and processor downtime.

## Verification

- Five views enter active browser; membership expires without further activity.
- A cart becomes abandoned after the documented threshold without a sixth event.
- Matching order completion removes abandonment; another cart remains independent.
- Late completion corrects eligibility.
- Duplicate events and restart/replay preserve memberships.
- Three distinct orders create repeat-customer membership.
- Missing or revoked consent produces a clear ineligibility reason.

Use a configurable short threshold for tests and a documented ten-minute default for the demo.

## Deliverables and exit criteria

Deliver versioned rule definitions, timer/recovery tests, the targeting API and a deterministic add-cart/abandon/complete scenario. This completes the first backend MVP; it establishes eligibility, not evidence that a reminder improves conversion.

Next: [Phase 5 â€” Demo and analytics](phase-5-demo.md).
