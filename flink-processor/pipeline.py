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
    'flink-sql-connector-kafka-4.0.1-2.0.jar',
    'flink-connector-jdbc-core-4.0.0-2.0.jar',
    'flink-connector-jdbc-postgres-4.0.0-2.0.jar',
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