# Grab-Inspired Kafka–Flink Event Processing & Observability Pipeline

A local, end-to-end streaming data project that simulates user activity, processes it with Apache Flink, separates invalid records into a dead-letter queue, and exposes operational metrics through Prometheus and Grafana.

The project is inspired by high-volume super-app event streams such as ride requests, food orders, payments, and grocery orders. It is intended as a hands-on demonstration of event-driven architecture, stream validation, enrichment, fault isolation, persistence, and observability.

## Highlights

- Produces realistic JSON events to Kafka at approximately two events per second.
- Deliberately injects about 20% invalid events to exercise failure handling.
- Validates and enriches events in a single PyFlink streaming job.
- Writes valid records to PostgreSQL and invalid records to a Kafka DLQ.
- Exposes native Flink runtime metrics and separate application data-quality metrics.
- Monitors Kafka and PostgreSQL through dedicated Prometheus exporters.
- Provisions Prometheus as the default Grafana data source.
- Routes pipeline and component-health alerts to Alertmanager.

## Architecture

![Architecture of the Kafka and Flink event processing and pull-based observability pipeline](flink-processor/src/public/architecture.png)

The detailed monitoring design and migration notes are in [MONITORING_ARCHITECTURE.md](MONITORING_ARCHITECTURE.md).

### Data flow

1. The Python producer generates a mix of valid and intentionally malformed activity events.
2. Kafka stores those events in the `user-events` topic.
3. A PyFlink `StatementSet` reads the source once and routes it into two sinks:
   - valid events are categorized and written to PostgreSQL;
   - invalid events are annotated with an error reason and written to `user-events-dlq`.
4. Flink exposes native runtime metrics while a separate application endpoint exposes data-quality metrics.
5. Kafka Exporter and PostgreSQL Exporter expose infrastructure metrics.
6. Prometheus scrapes every endpoint, supplies Grafana, and routes alerts to Alertmanager.

## Technology Stack

| Layer | Technology | Purpose |
| --- | --- | --- |
| Event generation | Python, Faker | Generate representative user activity |
| Message broker | Confluent Kafka image 7.4.0 | Durable event ingestion and DLQ storage |
| Stream processing | Apache Flink / PyFlink 2.2.1 | Validate, enrich, and route events |
| Operational storage | PostgreSQL 15 | Persist successfully processed events |
| Metric exporters | Flink reporter, Kafka Exporter, PostgreSQL Exporter | Expose component-owned metrics |
| Monitoring | Prometheus 2.47.0 | Scrape metrics and evaluate alerts |
| Visualization | Grafana 10.1.0 | Explore and visualize pipeline health |
| Alert routing | Alertmanager 0.32.1 | Group, silence, and route alerts |
| Local infrastructure | Docker Compose | Run supporting services |

## Event Contract

Events are serialized as JSON:

```json
{
  "user_id": "user_4821",
  "event_type": "ride_request",
  "timestamp": 1787486400000,
  "amount": 24.5
}
```

Supported event types and their enriched categories are:

| Event type | Category |
| --- | --- |
| `food_order` | `FOOD` |
| `ride_request` | `TRANSPORT` |
| `payment` | `FINANCE` |
| `grocery_order` | `GROCERY` |

An event is valid when `user_id` is present, `event_type` is supported, and `amount` is non-negative. Invalid records are routed to the DLQ with one of `NULL_USER_ID`, `INVALID_EVENT_TYPE`, `NEGATIVE_AMOUNT`, or `MULTIPLE_ERRORS`.

## Getting Started

### Prerequisites

- Docker Desktop with Docker Compose
- Python 3.11
- Java 17 recommended for Flink 2.2 (Java 11 is also supported)
- Bash and `curl` for downloading connector JARs

All commands below are run from the `grab-se-backend` directory.

### 1. Create a Python environment

PowerShell:

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
pip install -r requirements.txt
```

macOS, Linux, or Git Bash:

```bash
python -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
pip install -r requirements.txt
```

### 2. Download the Flink connectors and metrics reporter

Run the helper from Git Bash, WSL, macOS, or Linux:

```bash
./download_jars.sh
```

`pipeline.py` expects the following connector files under `jars/`:

```text
flink-sql-connector-kafka-4.0.1-2.0.jar
flink-connector-jdbc-core-4.0.0-2.0.jar
flink-connector-jdbc-postgres-4.0.0-2.0.jar
postgresql-42.6.0.jar
```

It also installs `flink-metrics-prometheus-2.2.1.jar` under `plugins/prometheus/`. The pipeline fails early with a clear error when the reporter is missing.

### 3. Start the infrastructure

```bash
docker compose up -d
docker compose ps
```

Kafka can take a few seconds to become ready. Confirm the broker is responding:

```bash
docker exec kafka kafka-topics --bootstrap-server localhost:9092 --list
```

### 4. Start the Flink pipeline

Open a new terminal, activate the virtual environment, and run:

```bash
python flink-processor/pipeline.py
```

The job reads from the earliest available offset, so events published before startup will still be processed.

### 5. Start producing events

Open another terminal, activate the virtual environment, and run:

```bash
python producer/event_producer.py
```

The producer continues until you press `Ctrl+C`.

## Explore the Pipeline

### Service endpoints

| Service | URL / connection | Credentials |
| --- | --- | --- |
| Grafana | <http://localhost:3000> | `admin` / `admin` |
| Prometheus | <http://localhost:9090> | None |
| Alertmanager | <http://localhost:9093> | None |
| Application metrics | <http://localhost:8000/metrics> | None |
| Flink metrics | `http://localhost:9249/metrics`, `:9250/metrics` | None |
| Kafka Exporter | <http://localhost:9308/metrics> | None |
| PostgreSQL Exporter | <http://localhost:9187/metrics> | None |
| Kafka from host | `localhost:29092` | None |
| PostgreSQL | `localhost:5432/grabevents` | `grabuser` / `grabpass` |

Grafana automatically receives Prometheus as its default data source. The provisioning directory currently contains no dashboard JSON, so create a dashboard and add panels using the metrics below.

### Available metrics

| Metric | Meaning |
| --- | --- |
| `pipeline_events_processed_total` | Current count of valid rows in PostgreSQL |
| `pipeline_dlq_events_total` | Total end offset across DLQ partitions |
| `pipeline_dlq_rate` | DLQ events divided by all observed events |
| `pipeline_metrics_collection_success` | Health of each business-metric source collection |

Native Flink, Kafka, and PostgreSQL metric names are supplied by their respective reporters. Check <http://localhost:9090/targets> to verify every scrape job, then use `{job="flink"}`, `{job="kafka"}`, and `{job="postgresql"}` to explore them. Suggested Grafana panels include Flink throughput and restarts, Kafka consumer lag, PostgreSQL sessions, processed events, and DLQ percentage.

### Inspect processed records

```bash
docker exec postgres psql -U grabuser -d grabevents -c \
  "SELECT * FROM processed_events ORDER BY processed_at DESC LIMIT 10;"
```

### Inspect the dead-letter queue

```bash
docker exec kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic user-events-dlq \
  --from-beginning \
  --max-messages 5
```

Example DLQ record:

```json
{
  "user_id": null,
  "event_type": "payment",
  "timestamp": 1787486400000,
  "amount": 10.25,
  "error_reason": "NULL_USER_ID"
}
```

## Project Structure

```text
grab-se-backend/
├── docker-compose.yml                 # Local infrastructure
├── MONITORING_ARCHITECTURE.md         # Monitoring implementation guide
├── requirements.txt                   # Python dependencies
├── download_jars.sh                   # Connector download helper
├── producer/
│   └── event_producer.py              # Synthetic Kafka producer
├── flink-processor/
│   └── pipeline.py                    # PyFlink routing pipeline
├── monitoring/
│   ├── metrics.py                     # Direct application metrics endpoint
│   ├── alertmanager.yml               # Local alert routing
│   ├── prometheus.yml                 # Scrape configuration
│   ├── prometheus_alerts.yml          # Alert rules
│   └── grafana/provisioning/          # Data source and dashboard provisioning
└── sql/
    └── init.sql                       # PostgreSQL schema and indexes
```

## Stopping and Resetting

Stop the producer and Flink process with `Ctrl+C`, then stop the containers while preserving PostgreSQL and Grafana volumes:

```bash
docker compose down
```

To perform a clean reset and delete local container data:

```bash
docker compose down -v
```

## Development Notes

This project is configured for local learning and demonstration. Kafka uses plaintext communication and a single broker, credentials are committed development defaults, Flink runs at parallelism `1`, and Kafka retains logs for one hour. Production deployments should use secrets management, authenticated and encrypted connections, replicated brokers, checkpointing, schema management, durable metric collection, and environment-based configuration.

## Ideas for Extension

- Add a provisioned Grafana dashboard and external Alertmanager receiver.
- Introduce Avro or Protobuf with a schema registry.
- Add Flink checkpoints and restart strategies.
- Containerize the producer and PyFlink job.
- Add event-time windows for revenue and activity aggregates.
- Replace hard-coded settings with environment variables.
- Add automated tests for validation and routing behavior.
