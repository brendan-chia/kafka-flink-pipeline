# Phase 6 â€” Reliability and portfolio evidence

Status: planned. Dependency: Phase 5 supplies a complete instrumented journey.

## Objective

Demonstrate correctness, recovery and measured performance using reproducible scenarios.

## Planned implementation

1. Add a finite load generator with configurable users, sessions, rate, duplicates, invalid events and delayed events.
2. Define expected profiles/segments/funnel outcomes independently of the processor.
3. Measure ingestion throughput, p95 acknowledged ingestion latency, event-to-profile freshness and serving API latency.
4. Restart/fail the JourneyPulse processor and verify checkpoint restoration, source replay and durable state.
5. Exercise Kafka and PostgreSQL interruption; verify retry/backpressure behavior and eventual reconciliation.
6. Add bounded-label metrics, alerts and recovery/runbook documentation.
7. Deliver an architecture diagram, concise interview demonstration, actual results and remaining limitations.

Reuse existing monitoring and recovery patterns where applicable. Separate consumer groups, checkpoints, datasets and reports so the existing transaction benchmarks remain meaningful.

## Benchmark method

Record software versions, CPU/RAM, partition count, processing parallelism, checkpoint interval, deduplication retention, load duration and offered rate.

Separate successful acknowledgement latency from failed requests. Separate event-to-profile freshness from source-event age; intentional client delay should not be misreported as processing delay. Report sample counts and percentile calculation methods.

Evaluate recovery by both time and correctness: resumed processing with wrong counts or stale memberships is a failure. Declare the tested fault scope; TaskManager restart evidence does not establish broker replication or JobManager high availability.

## Acceptance scenarios

- Normal complete journey produces the expected profile and funnel.
- Duplicate and invalid deliveries have the documented effects.
- Out-of-order completion corrects an abandoned cart.
- Consent revocation changes eligibility promptly.
- A quiet stream still expires time-dependent segments.
- Recovery produces no unexpected count inflation or stale segment resurrection.
- A sustained load run reports actual rates, percentiles, failures and bottlenecks.

## Deliverables and exit criteria

Deliver generated benchmark/recovery reports, the commands to reproduce them, an operational runbook and the final JourneyPulse README. State actual delivery guarantees, deduplication boundaries and failure limitations. Do not claim universal end-to-end exactly-once processing.

The interview demo should show: browse, add cart, observe validation/profile, show abandonment and consent-aware eligibility, inject duplicate/invalid events, demonstrate recovery, then complete the cart.
