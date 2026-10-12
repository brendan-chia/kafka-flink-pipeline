# ADR 005: Add JourneyPulse as an isolated module on shared local infrastructure

Status: accepted. Date: 2026-10-12.

The existing project demonstrates reliable transaction processing and DataLens investigations. JourneyPulse needs browser clickstream contracts, customer state and customer-serving APIs. The transaction contract requires monetary fields and accepts four transaction event types, so it cannot directly model browsing and cart activity.

Keep one repository and reuse the local Kafka, PostgreSQL and monitoring infrastructure. Add dedicated JourneyPulse topics, contracts, PostgreSQL schema, setup overlay and, in later phases, separate gateway, processor and serving services. The base Compose file and transaction/DataLens behavior are independent of this optional module.

Phase 1 freezes a strict Draft 2020-12 clickstream schema with five event names. It supplies example validation and a five-row contract catalogue. Setup creates missing resources and verifies matching existing resources; it refuses incompatible topic settings or changed registered v1 definitions. New versions require deliberate rollout. Schema immutability is enforced by the bootstrap operation, not by a database trigger or access-control model.

Topic creation and catalogue migration are not atomic across Kafka/PostgreSQL. Database changes are transactional; rerunning setup completes partial infrastructure creation. Logical separation within shared development containers is sufficient for this portfolio workflow; separate deployments and security boundaries remain future requirements.

The source and quarantine topics use three partitions, one replica and seven-day retention for the local broker. Source records will be keyed by user ID in Phase 2. Broker append time is distinct from client event time, which remains in the payload. The initial catalogue does not implement a dynamic registry API.

[Module guide](../../journeypulse/README.md) and [phase guides](../journeypulse/phase-1-foundation.md) describe implementation and future acceptance checks.

References: [JSON Schema 2020-12](https://json-schema.org/draft/2020-12), [Confluent Kafka basics](https://docs.confluent.io/platform/7.6/kafka/kafka-basics.html).
