# Real-Time Event Processor: Apache Flink + Kafka + PostgreSQL
### A Portfolio Project for Grab GrabX Internship

---

## What You'll Build

A local streaming pipeline that:
1. **Produces** fake Grab-like user activity events (food orders, rides, payments) to Kafka
2. **Consumes** events with Apache Flink, validates and enriches them
3. **Sinks** valid events to PostgreSQL
4. **Routes** invalid events to a Kafka Dead-Letter Queue (DLQ) topic

**All tools used are 100% free and open source.**

---

## Prerequisites

Install these before starting:

| Tool | Download | Why |
|------|----------|-----|
| Docker Desktop | https://www.docker.com/products/docker-desktop | Runs Kafka + PostgreSQL |
| Python 3.8–3.11 | https://www.python.org/downloads | PyFlink + producer script |
| Java 11 (JDK) | https://adoptium.net | Required by PyFlink internally |
| Git | https://git-scm.com | Version control |

**Verify your setup before proceeding:**
```bash
docker --version        # Should print Docker version
python3 --version       # Should be 3.8 – 3.11
java -version           # Should be Java 11
```

> ⚠️ PyFlink does NOT work with Java 17+ in some versions. Stick to Java 11.

---

## Project Structure

Create this folder structure:

```
grab-flink-pipeline/
├── docker-compose.yml
├── requirements.txt
├── download_jars.sh
├── .gitignore
├── sql/
│   └── init.sql
├── jars/               ← (empty for now, jars go here)
├── producer/
│   └── event_producer.py
└── flink_job/
    └── pipeline.py
```

Run this to create it:
```bash
mkdir -p grab-flink-pipeline/{sql,jars,producer,flink_job}
cd grab-flink-pipeline
```

---

## Step 1: Docker Compose — Kafka + PostgreSQL

Create `docker-compose.yml`:

```yaml
version: '3.8'

services:
  zookeeper:
    image: confluentinc/cp-zookeeper:7.4.0
    container_name: zookeeper
    environment:
      ZOOKEEPER_CLIENT_PORT: 2181
      ZOOKEEPER_TICK_TIME: 2000
    ports:
      - "2181:2181"

  kafka:
    image: confluentinc/cp-kafka:7.4.0
    container_name: kafka
    depends_on:
      - zookeeper
    ports:
      - "9092:9092"
    environment:
      KAFKA_BROKER_ID: 1
      KAFKA_ZOOKEEPER_CONNECT: zookeeper:2181
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://localhost:9092
      KAFKA_LISTENERS: PLAINTEXT://0.0.0.0:9092
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
      KAFKA_AUTO_CREATE_TOPICS_ENABLE: 'true'
      KAFKA_LOG_RETENTION_HOURS: 1

  postgres:
    image: postgres:15
    container_name: postgres
    environment:
      POSTGRES_DB: grabevents
      POSTGRES_USER: grabuser
      POSTGRES_PASSWORD: grabpass
    ports:
      - "5432:5432"
    volumes:
      - ./sql/init.sql:/docker-entrypoint-initdb.d/init.sql
      - postgres_data:/var/lib/postgresql/data

volumes:
  postgres_data:
```

---

## Step 2: PostgreSQL Schema

Create `sql/init.sql`:

```sql
-- Table for valid, processed events
CREATE TABLE IF NOT EXISTS processed_events (
    id          SERIAL PRIMARY KEY,
    user_id     VARCHAR(100)   NOT NULL,
    event_type  VARCHAR(50)    NOT NULL,
    amount      DECIMAL(10, 2) NOT NULL,
    category    VARCHAR(50),
    processed_at TIMESTAMP      DEFAULT CURRENT_TIMESTAMP
);

-- Index for fast lookups by user
CREATE INDEX IF NOT EXISTS idx_processed_user_id ON processed_events(user_id);
CREATE INDEX IF NOT EXISTS idx_processed_event_type ON processed_events(event_type);
```

> The DLQ goes to a **Kafka topic** (not Postgres) — this is more realistic and industry-standard.

---

## Step 3: Python Dependencies

Create `requirements.txt`:

```
apache-flink==1.17.0
kafka-python==2.0.2
psycopg2-binary==2.9.9
faker==19.13.0
```

Install them:
```bash
pip install -r requirements.txt
```

> `faker` generates realistic fake names/IDs. `psycopg2-binary` lets you query Postgres to verify results.

---

## Step 4: Download Flink JARs

PyFlink needs connector JARs to talk to Kafka and PostgreSQL. Create `download_jars.sh`:

```bash
#!/bin/bash

# Create jars directory
mkdir -p jars
cd jars

echo "Downloading Flink Kafka connector..."
curl -L -O https://repo.maven.apache.org/maven2/org/apache/flink/flink-sql-connector-kafka/1.17.0/flink-sql-connector-kafka-1.17.0.jar

echo "Downloading Flink JDBC connector..."
curl -L -O https://repo.maven.apache.org/maven2/org/apache/flink/flink-connector-jdbc/3.1.0-1.17/flink-connector-jdbc-3.1.0-1.17.jar

echo "Downloading PostgreSQL driver..."
curl -L -O https://repo1.maven.org/maven2/org/postgresql/postgresql/42.6.0/postgresql-42.6.0.jar

echo "All JARs downloaded:"
ls -lh *.jar
```

Run it:
```bash
chmod +x download_jars.sh
./download_jars.sh
```

You should see 3 `.jar` files in the `jars/` folder.

---

## Step 5: Event Producer

This script generates fake Grab-like events and sends them to Kafka.
A portion of events are intentionally invalid (negative amounts, unknown event types, null user IDs) to demonstrate the DLQ.

Create `producer/event_producer.py`:

```python
"""
Grab Event Producer
-------------------
Simulates a stream of user activity events being sent to Kafka.
Intentionally injects ~20% invalid events to demonstrate DLQ routing.
"""

import json
import random
import time
import logging
from datetime import datetime

from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable
from faker import Faker

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [PRODUCER] %(message)s'
)
logger = logging.getLogger(__name__)

fake = Faker()

# ── Config ────────────────────────────────────────────────────────────────────
KAFKA_BOOTSTRAP = 'localhost:9092'
TOPIC = 'user-events'
EVENTS_PER_SECOND = 2   # Adjust as needed

# Valid event types that the Flink pipeline accepts
VALID_EVENT_TYPES = ['food_order', 'ride_request', 'payment', 'grocery_order']

# Invalid types injected to test DLQ
INVALID_EVENT_TYPES = ['HACK_ATTEMPT', 'unknown_event', 'null_type', '']


def connect_kafka(retries: int = 10, delay: int = 3) -> KafkaProducer:
    """Retry connecting to Kafka — it takes a few seconds to start."""
    for attempt in range(retries):
        try:
            producer = KafkaProducer(
                bootstrap_servers=[KAFKA_BOOTSTRAP],
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                acks='all',          # Wait for broker acknowledgement
                retries=3,
            )
            logger.info("Connected to Kafka successfully.")
            return producer
        except NoBrokersAvailable:
            logger.warning(f"Kafka not ready yet. Retrying in {delay}s... ({attempt + 1}/{retries})")
            time.sleep(delay)
    raise RuntimeError("Could not connect to Kafka after multiple retries.")


def generate_valid_event() -> dict:
    """Generates a realistic, valid Grab user activity event."""
    return {
        'user_id': f'user_{fake.numerify("####")}',
        'event_type': random.choice(VALID_EVENT_TYPES),
        'timestamp': int(datetime.utcnow().timestamp() * 1000),  # milliseconds
        'amount': round(random.uniform(1.0, 150.0), 2),
    }


def generate_invalid_event() -> dict:
    """Generates an intentionally malformed event for DLQ testing."""
    invalid_choice = random.choice(['null_user', 'bad_type', 'negative_amount', 'multiple_errors'])

    if invalid_choice == 'null_user':
        return {
            'user_id': None,
            'event_type': random.choice(VALID_EVENT_TYPES),
            'timestamp': int(datetime.utcnow().timestamp() * 1000),
            'amount': round(random.uniform(1.0, 50.0), 2),
        }
    elif invalid_choice == 'bad_type':
        return {
            'user_id': f'user_{fake.numerify("####")}',
            'event_type': random.choice(INVALID_EVENT_TYPES),
            'timestamp': int(datetime.utcnow().timestamp() * 1000),
            'amount': round(random.uniform(1.0, 50.0), 2),
        }
    elif invalid_choice == 'negative_amount':
        return {
            'user_id': f'user_{fake.numerify("####")}',
            'event_type': random.choice(VALID_EVENT_TYPES),
            'timestamp': int(datetime.utcnow().timestamp() * 1000),
            'amount': round(random.uniform(-50.0, -0.01), 2),
        }
    else:  # multiple_errors
        return {
            'user_id': None,
            'event_type': random.choice(INVALID_EVENT_TYPES),
            'timestamp': int(datetime.utcnow().timestamp() * 1000),
            'amount': round(random.uniform(-10.0, -0.01), 2),
        }


def main():
    producer = connect_kafka()
    sent_count = 0
    invalid_count = 0

    logger.info(f"Sending events to topic '{TOPIC}' at ~{EVENTS_PER_SECOND} events/sec.")
    logger.info("Press Ctrl+C to stop.\n")

    try:
        while True:
            # Inject ~20% invalid events
            is_invalid = random.random() < 0.20

            event = generate_invalid_event() if is_invalid else generate_valid_event()
            producer.send(TOPIC, value=event)
            sent_count += 1

            if is_invalid:
                invalid_count += 1
                logger.info(f"[INVALID] #{sent_count} → {event}")
            else:
                logger.info(f"[VALID]   #{sent_count} → {event}")

            if sent_count % 20 == 0:
                logger.info(f"── Summary: {sent_count} total sent, {invalid_count} invalid ({invalid_count/sent_count*100:.0f}%) ──")

            time.sleep(1.0 / EVENTS_PER_SECOND)

    except KeyboardInterrupt:
        logger.info(f"\nStopped. Total: {sent_count} events sent, {invalid_count} invalid.")
    finally:
        producer.flush()
        producer.close()


if __name__ == '__main__':
    main()
```

---

## Step 6: Flink Pipeline

This is the core of the project. It:
- Reads from the `user-events` Kafka topic
- Validates each event (null user, bad event type, negative amount)
- Enriches valid events with a `category` label (using a Python UDF)
- Writes valid events to PostgreSQL
- Routes invalid events to a `user-events-dlq` Kafka topic

Create `flink_job/pipeline.py`:

```python
"""
Grab Event Processing Pipeline
-------------------------------
PyFlink Table API pipeline that:
  1. Reads user activity events from Kafka
  2. Validates and enriches valid events
  3. Sinks valid events → PostgreSQL
  4. Sinks invalid events → Kafka DLQ topic

Run with:
  python flink_job/pipeline.py
"""

import os
import logging

from pyflink.table import EnvironmentSettings, TableEnvironment
from pyflink.table.udf import udf
from pyflink.table.types import DataTypes

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [FLINK] %(message)s'
)
logger = logging.getLogger(__name__)

# ── Config ─────────────────────────────────────────────────────────────────────
KAFKA_BOOTSTRAP     = 'localhost:9092'
SOURCE_TOPIC        = 'user-events'
DLQ_TOPIC           = 'user-events-dlq'
CONSUMER_GROUP      = 'flink-grab-consumer'

POSTGRES_URL        = 'jdbc:postgresql://localhost:5432/grabevents'
POSTGRES_USER       = 'grabuser'
POSTGRES_PASS       = 'grabpass'
POSTGRES_TABLE      = 'processed_events'

# Valid event types — anything else goes to DLQ
VALID_TYPES_SQL     = "('food_order', 'ride_request', 'payment', 'grocery_order')"

# ── UDF: Event Type → Category ──────────────────────────────────────────────────
CATEGORY_MAP = {
    'food_order':     'FOOD',
    'ride_request':   'TRANSPORT',
    'payment':        'FINANCE',
    'grocery_order':  'GROCERY',
}


@udf(result_type=DataTypes.STRING())
def categorize(event_type: str) -> str:
    """Enriches an event_type string with a human-readable category label."""
    if event_type is None:
        return 'UNKNOWN'
    return CATEGORY_MAP.get(event_type, 'UNKNOWN')


# ── JAR Setup ──────────────────────────────────────────────────────────────────
def get_jar_uris() -> str:
    """Build semicolon-separated list of JAR file:// URIs for Flink connectors."""
    project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    jar_dir = os.path.join(project_root, 'jars')

    jar_names = [
        'flink-sql-connector-kafka-1.17.0.jar',
        'flink-connector-jdbc-3.1.0-1.17.jar',
        'postgresql-42.6.0.jar',
    ]

    uris = []
    for name in jar_names:
        path = os.path.join(jar_dir, name)
        if not os.path.exists(path):
            raise FileNotFoundError(
                f"JAR not found: {path}\n"
                f"Run ./download_jars.sh first."
            )
        uris.append(f"file://{path}")

    return ";".join(uris)


# ── Table Environment Setup ─────────────────────────────────────────────────────
def create_table_env() -> TableEnvironment:
    env_settings = EnvironmentSettings.in_streaming_mode()
    t_env = TableEnvironment.create(env_settings)

    # Register JAR connectors
    t_env.get_config().set("pipeline.jars", get_jar_uris())

    # Parallelism 1 is fine for local development
    t_env.get_config().set("parallelism.default", "1")

    # Register the categorize UDF
    t_env.create_temporary_function("categorize", categorize)

    return t_env


# ── Table Definitions ──────────────────────────────────────────────────────────
def create_source_table(t_env: TableEnvironment):
    """Kafka source: reads raw JSON events from user-events topic."""
    t_env.execute_sql(f"""
        CREATE TABLE raw_events (
            user_id    STRING,
            event_type STRING,
            `timestamp` BIGINT,
            amount     DOUBLE,
            proc_time  AS PROCTIME()
        ) WITH (
            'connector'                     = 'kafka',
            'topic'                         = '{SOURCE_TOPIC}',
            'properties.bootstrap.servers'  = '{KAFKA_BOOTSTRAP}',
            'properties.group.id'           = '{CONSUMER_GROUP}',
            'scan.startup.mode'             = 'earliest-offset',
            'format'                        = 'json',
            'json.ignore-parse-errors'      = 'true'
        )
    """)


def create_postgres_sink(t_env: TableEnvironment):
    """PostgreSQL JDBC sink for valid, enriched events."""
    t_env.execute_sql(f"""
        CREATE TABLE processed_events (
            user_id      STRING,
            event_type   STRING,
            amount       DOUBLE,
            category     STRING,
            processed_at TIMESTAMP(3)
        ) WITH (
            'connector'  = 'jdbc',
            'url'        = '{POSTGRES_URL}',
            'table-name' = '{POSTGRES_TABLE}',
            'username'   = '{POSTGRES_USER}',
            'password'   = '{POSTGRES_PASS}',
            'driver'     = 'org.postgresql.Driver'
        )
    """)


def create_dlq_sink(t_env: TableEnvironment):
    """Kafka DLQ sink for invalid/malformed events."""
    t_env.execute_sql(f"""
        CREATE TABLE dlq_events (
            user_id      STRING,
            event_type   STRING,
            `timestamp`  BIGINT,
            amount       DOUBLE,
            error_reason STRING
        ) WITH (
            'connector'                     = 'kafka',
            'topic'                         = '{DLQ_TOPIC}',
            'properties.bootstrap.servers'  = '{KAFKA_BOOTSTRAP}',
            'format'                        = 'json'
        )
    """)


# ── Pipeline Logic ─────────────────────────────────────────────────────────────
def build_and_run(t_env: TableEnvironment):
    """
    StatementSet runs both inserts as a SINGLE Flink job,
    allowing the Kafka source to be shared (no double-reading).
    """
    stmt_set = t_env.create_statement_set()

    # ── INSERT 1: Valid events → PostgreSQL ────────────────────────────────────
    stmt_set.add_insert_sql(f"""
        INSERT INTO processed_events
        SELECT
            user_id,
            event_type,
            amount,
            categorize(event_type)  AS category,
            CURRENT_TIMESTAMP       AS processed_at
        FROM raw_events
        WHERE user_id    IS NOT NULL
          AND event_type IN {VALID_TYPES_SQL}
          AND amount     >= 0.0
    """)

    # ── INSERT 2: Invalid events → Kafka DLQ ──────────────────────────────────
    # CASE statement identifies the FIRST failing validation rule.
    stmt_set.add_insert_sql(f"""
        INSERT INTO dlq_events
        SELECT
            user_id,
            event_type,
            `timestamp`,
            amount,
            CASE
                WHEN user_id    IS NULL                  THEN 'NULL_USER_ID'
                WHEN event_type NOT IN {VALID_TYPES_SQL} THEN 'INVALID_EVENT_TYPE'
                WHEN amount     < 0.0                    THEN 'NEGATIVE_AMOUNT'
                ELSE                                          'MULTIPLE_ERRORS'
            END AS error_reason
        FROM raw_events
        WHERE user_id    IS NULL
           OR event_type NOT IN {VALID_TYPES_SQL}
           OR amount     < 0.0
    """)

    logger.info("Submitting Flink job...")
    result = stmt_set.execute()
    logger.info(f"Pipeline running. Waiting for events...")

    # Block until the job is cancelled (Ctrl+C)
    try:
        result.get_job_client().get_job_execution_result().result()
    except KeyboardInterrupt:
        logger.info("Pipeline stopped by user.")


# ── Entry Point ────────────────────────────────────────────────────────────────
def main():
    logger.info("=" * 60)
    logger.info("  Grab Real-Time Event Processing Pipeline")
    logger.info("=" * 60)

    t_env = create_table_env()

    logger.info("Creating source table (Kafka)...")
    create_source_table(t_env)

    logger.info("Creating PostgreSQL sink...")
    create_postgres_sink(t_env)

    logger.info("Creating Kafka DLQ sink...")
    create_dlq_sink(t_env)

    build_and_run(t_env)


if __name__ == '__main__':
    main()
```

---

## Step 7: .gitignore

Create `.gitignore`:

```
# JARs — too large for GitHub, re-downloaded via download_jars.sh
jars/

# Python
__pycache__/
*.pyc
*.pyo
.venv/
venv/

# Flink logs
log/
*.log

# Docker volumes
postgres_data/
```

---

## Step 8: Run Everything

Open **three separate terminal windows**.

### Terminal 1 — Start Infrastructure
```bash
cd grab-flink-pipeline
docker compose up -d

# Wait ~15 seconds for Kafka to fully start, then verify:
docker compose ps
# All three services (zookeeper, kafka, postgres) should show "Up"
```

**Verify Kafka is ready:**
```bash
docker exec kafka kafka-topics --bootstrap-server localhost:9092 --list
# Should return empty or a list — no errors means Kafka is up
```

### Terminal 2 — Start the Event Producer
```bash
cd grab-flink-pipeline
python producer/event_producer.py
```

You should see events streaming:
```
2026-05-26 [PRODUCER] Connected to Kafka successfully.
2026-05-26 [PRODUCER] [VALID]   #1  → {'user_id': 'user_4821', 'event_type': 'food_order', ...}
2026-05-26 [PRODUCER] [INVALID] #2  → {'user_id': None, 'event_type': 'food_order', ...}
```

### Terminal 3 — Start the Flink Pipeline
```bash
cd grab-flink-pipeline
python flink_job/pipeline.py
```

You should see:
```
2026-05-26 [FLINK] Creating source table (Kafka)...
2026-05-26 [FLINK] Creating PostgreSQL sink...
2026-05-26 [FLINK] Creating Kafka DLQ sink...
2026-05-26 [FLINK] Submitting Flink job...
2026-05-26 [FLINK] Pipeline running. Waiting for events...
```

---

## Step 9: Verify Results

Let it run for ~30 seconds, then verify in a 4th terminal:

### Check PostgreSQL — valid processed events
```bash
docker exec -it postgres psql -U grabuser -d grabevents -c "
SELECT event_type, category, COUNT(*) as count, AVG(amount) as avg_amount
FROM processed_events
GROUP BY event_type, category
ORDER BY count DESC;
"
```

Expected output:
```
  event_type   | category  | count | avg_amount
---------------+-----------+-------+------------
 food_order    | FOOD      |    18 |   73.21
 ride_request  | TRANSPORT |    14 |   68.44
 payment       | FINANCE   |    12 |   80.12
 grocery_order | GROCERY   |    10 |   65.33
```

### Check Kafka DLQ — invalid events
```bash
docker exec kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic user-events-dlq \
  --from-beginning \
  --max-messages 5
```

Expected output (one JSON per line):
```json
{"user_id":null,"event_type":"food_order","timestamp":1716700000000,"amount":23.5,"error_reason":"NULL_USER_ID"}
{"user_id":"user_1234","event_type":"HACK_ATTEMPT","timestamp":1716700001000,"amount":10.0,"error_reason":"INVALID_EVENT_TYPE"}
{"user_id":"user_5678","event_type":"ride_request","timestamp":1716700002000,"amount":-15.0,"error_reason":"NEGATIVE_AMOUNT"}
```

### Check Kafka source topic message count
```bash
docker exec kafka kafka-run-class kafka.tools.GetOffsetShell \
  --broker-list localhost:9092 \
  --topic user-events \
  --time -1
```

---

## Step 10: Troubleshooting

### "NoBrokersAvailable" from producer
Kafka isn't ready yet. Wait 15–20 seconds and retry.

### "FileNotFoundError: JAR not found"
Run `./download_jars.sh` first and confirm 3 JARs are in `jars/`.

### "ClassNotFoundException: org.postgresql.Driver"
The PostgreSQL JAR isn't being picked up. Make sure all 3 JARs are present, and the paths in `get_jar_uris()` are correct. Print them with:
```python
print(get_jar_uris())
```

### Flink job exits immediately without error
This usually means the Kafka topic is empty. Make sure the producer is running first.

### Port conflict (5432 or 9092 already in use)
Stop any local PostgreSQL or Kafka instance, or change the port in `docker-compose.yml`.

### PyFlink "Java gateway" error
Wrong Java version. Run `java -version` — must be Java 11. Install from https://adoptium.net and set `JAVA_HOME` if needed.

---

## Step 11: Stop Everything

```bash
# Stop producer and Flink job: Ctrl+C in each terminal

# Stop and remove Docker containers (keeps data)
docker compose down

# Stop and WIPE all data (clean reset)
docker compose down -v
```

---

## Step 12: GitHub README — What to Write

Your README is what a Grab recruiter will actually read. Use this structure:

```markdown
## Grab-Style Real-Time Event Processing Pipeline

A streaming data pipeline built with **Apache Flink + Kafka + PostgreSQL** that 
processes user activity events (food orders, rides, payments) in real time.

### Architecture
[Producer] → [Kafka: user-events] → [Flink PyFlink Job] → [PostgreSQL: processed_events]
                                                         ↘ [Kafka: user-events-dlq]

### What it demonstrates
- Apache Flink Table API with multiple sinks (StatementSet)
- Kafka as event source and DLQ sink
- Python UDF for real-time event enrichment
- JDBC sink to PostgreSQL
- Dead-Letter Queue pattern for malformed event handling
- Dockerised infrastructure (Kafka, PostgreSQL)

### Tech Stack
Python · Apache Flink 1.17 (PyFlink) · Apache Kafka · PostgreSQL · Docker
```

---

## What This Shows Interviewers at Grab

| Concept | How this project demonstrates it |
|---|---|
| Stream processing | Events flow continuously, not batch |
| Flink Table API | StatementSet with two sinks, shared Kafka source |
| Kafka fundamentals | Producer, consumer group, DLQ topic |
| Data quality | Validation rules with clear error classification |
| Enrichment | Python UDF mapping event_type → category |
| JDBC sink | PostgreSQL integration via Flink connector |
| Observability mindset | DLQ pattern = you think about failure cases |
| Docker / infra | docker-compose.yml shows infra awareness |
