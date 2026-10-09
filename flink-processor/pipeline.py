"""Validate every Kafka record, upsert valid events, and preserve invalid payloads."""

import logging
import os
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
FLINK_PLUGINS_DIR = PROJECT_ROOT / 'plugins'
os.environ.setdefault('FLINK_PLUGINS_DIR', str(FLINK_PLUGINS_DIR))
sys.path.insert(0, str(PROJECT_ROOT))

import lakehouse_config
lakehouse_config.configure_gateway_classpath(PROJECT_ROOT)

from pyflink.common import Configuration
from pyflink.table import EnvironmentSettings, TableEnvironment
from pyflink.table.udf import udf, udtf
from pyflink.table.types import DataTypes
from event_contract import decode_event, epoch_milliseconds
from monitoring.metrics import start_metrics_server

logging.basicConfig(level=logging.INFO, format='%(asctime)s [FLINK] %(message)s')
logger = logging.getLogger(__name__)
KAFKA_BOOTSTRAP = os.getenv('KAFKA_BOOTSTRAP', 'localhost:29092')
SOURCE_TOPIC = os.getenv('SOURCE_TOPIC', 'user-events')
DLQ_TOPIC = os.getenv('DLQ_TOPIC', 'user-events-dlq')
CONSUMER_GROUP = os.getenv('CONSUMER_GROUP', 'flink-grab-consumer')
FLINK_METRICS_PORTS = os.getenv('FLINK_METRICS_PORTS', '9249-9250')
POSTGRES_URL = os.getenv('POSTGRES_URL', 'jdbc:postgresql://localhost:5432/grabevents')
POSTGRES_USER = os.getenv('POSTGRES_USER', 'grabuser')
POSTGRES_PASS = os.getenv('POSTGRES_PASS', 'grabpass')
POSTGRES_TABLE = 'processed_events'
PG_CONN_PARAMS = {
    'host': os.getenv('POSTGRES_HOST', 'localhost'),
    'port': int(os.getenv('POSTGRES_PORT', '5432')),
    'dbname': os.getenv('POSTGRES_DB', 'grabevents'),
    'user': POSTGRES_USER, 'password': POSTGRES_PASS,
}
WINDOW_SECONDS = int(os.getenv('EVENT_WINDOW_SECONDS', '300'))
WATERMARK_SECONDS = int(os.getenv('EVENT_WATERMARK_SECONDS', '10'))
IDLE_SECONDS = int(os.getenv('EVENT_IDLE_SECONDS', '60'))
FUTURE_SKEW_SECONDS = int(os.getenv('EVENT_MAX_FUTURE_SKEW_SECONDS', '60'))
if not 0 < WINDOW_SECONDS <= 86400 or not 0 <= WATERMARK_SECONDS <= 86400 or IDLE_SECONDS <= 0:
    raise ValueError('Window/idleness must be positive and watermark delay non-negative.')
if FUTURE_SKEW_SECONDS < 0:
    raise ValueError('EVENT_MAX_FUTURE_SKEW_SECONDS must be non-negative.')


def decode_received_event(payload, ingested_at=None):
    received_ms = epoch_milliseconds(ingested_at) if ingested_at is not None else None
    return decode_event(payload, received_ms, FUTURE_SKEW_SECONDS * 1000)


@udf(result_type=DataTypes.BIGINT())
def validated_timestamp(payload, ingested_at=None):
    # Flink's WatermarkAssigner rejects null rowtime. Invalid records receive
    # epoch zero, never a malformed/future timestamp, and are filtered before windows.
    return decode_received_event(payload, ingested_at)[3] or 0

@udtf(result_types=[DataTypes.STRING(), DataTypes.STRING(), DataTypes.STRING(),
                    DataTypes.BIGINT(), DataTypes.DECIMAL(10, 2), DataTypes.STRING(),
                    DataTypes.STRING(), DataTypes.STRING(), DataTypes.STRING(),
                    DataTypes.STRING()])
def parse_event(payload, ingested_at=None):
    yield decode_received_event(payload, ingested_at)


def sql_literal(value):
    return value.replace("'", "''")


def sql_interval(seconds):
    days, rest = divmod(seconds, 86400)
    hours, rest = divmod(rest, 3600)
    minutes, seconds = divmod(rest, 60)
    return f"INTERVAL '{days} {hours:02}:{minutes:02}:{seconds:02}' DAY TO SECOND"


def get_jar_uris():
    names = ['flink-sql-connector-kafka-4.0.1-2.0.jar',
             'flink-connector-jdbc-core-4.0.0-2.0.jar',
             'flink-connector-jdbc-postgres-4.0.0-2.0.jar', 'postgresql-42.6.0.jar']
    if lakehouse_config.enabled():
        names.extend(lakehouse_config.JARS)
    paths = [PROJECT_ROOT / 'jars' / name for name in names]
    for path in paths:
        if not path.exists():
            raise FileNotFoundError(f'JAR not found: {path}. Run ./download_jars.sh and, for Iceberg, python scripts/download_lakehouse_jars.py.')
    return ';'.join(path.as_uri() for path in paths)


def ensure_prometheus_reporter_plugin():
    path = FLINK_PLUGINS_DIR / 'prometheus' / 'flink-metrics-prometheus-2.2.1.jar'
    if not path.exists():
        raise FileNotFoundError(f'Flink reporter not found: {path}. Run ./download_jars.sh first.')


def create_table_env():
    jar_uris = get_jar_uris()
    if lakehouse_config.enabled():
        from pyflink.java_gateway import get_gateway
        version = get_gateway().jvm.java.lang.System.getProperty('java.specification.version')
        if int(version.split('.')[-1]) < 17:
            raise RuntimeError('Iceberg 1.12 requires Java 17+. Set JAVA_HOME to your Java 17 installation before starting Python.')
    state_dir = PROJECT_ROOT / '.flink-state'
    config = Configuration()
    settings = {
        'metrics.reporters': 'prom',
        'metrics.reporter.prom.factory.class': 'org.apache.flink.metrics.prometheus.PrometheusReporterFactory',
        'metrics.reporter.prom.port': FLINK_METRICS_PORTS,
        'metrics.reporter.prom.scope.variables.additional': 'environment:local,pipeline:grab_events',
        'execution.checkpointing.interval': os.getenv('FLINK_CHECKPOINT_INTERVAL', '30 s'),
        'execution.checkpointing.mode': 'EXACTLY_ONCE',
        'execution.checkpointing.storage': 'filesystem',
        'execution.checkpointing.dir': os.getenv('FLINK_CHECKPOINT_DIR', (state_dir / 'checkpoints').as_uri()),
        'execution.checkpointing.savepoint-dir': os.getenv('FLINK_SAVEPOINT_DIR', (state_dir / 'savepoints').as_uri()),
        'execution.checkpointing.externalized-checkpoint-retention': 'RETAIN_ON_CANCELLATION',
        'execution.checkpointing.num-retained': '3',
        'execution.checkpointing.timeout': '2 min',
        'execution.checkpointing.min-pause': '5 s',
        'execution.checkpointing.max-concurrent-checkpoints': '1',
        'restart-strategy.type': 'fixed-delay',
        'restart-strategy.fixed-delay.attempts': '10',
        'restart-strategy.fixed-delay.delay': '10 s',
        'table.local-time-zone': 'UTC',
        'table.exec.uid.generation': 'ALWAYS',
        'python.executable': os.getenv('FLINK_PYTHON_EXECUTABLE', sys.executable),
        'table.exec.source.idle-timeout': f'{IDLE_SECONDS} s',
        'pipeline.auto-watermark-interval': '200 ms',
    }
    restore_path = os.getenv('FLINK_RESTORE_PATH')
    if restore_path:
        settings['execution.state-recovery.path'] = restore_path
        settings['execution.state-recovery.ignore-unclaimed-state'] = 'false'
        settings['execution.state-recovery.claim-mode'] = 'NO_CLAIM'
    for key, value in settings.items():
        config.set_string(key, value)
    env = TableEnvironment.create(EnvironmentSettings.new_instance().in_streaming_mode()
                                  .with_configuration(config).build())
    env.get_config().set('pipeline.jars', jar_uris)
    env.get_config().set('parallelism.default', os.getenv('FLINK_PARALLELISM', '1'))
    env.add_python_file(str(PROJECT_ROOT / 'event_contract.py'))
    env.create_temporary_system_function('parse_event', parse_event)
    env.create_temporary_system_function('validated_timestamp', validated_timestamp)
    logger.info('Checkpoint directory: %s; restore: %s', settings['execution.checkpointing.dir'], restore_path or 'fresh replay')
    return env


def create_source_table(env):
    env.execute_sql(f"""
        CREATE TABLE raw_events (
            raw_payload BYTES,
            source_topic STRING METADATA FROM 'topic' VIRTUAL,
            source_partition INT METADATA FROM 'partition' VIRTUAL,
            source_offset BIGINT METADATA FROM 'offset' VIRTUAL,
            ingested_at TIMESTAMP_LTZ(3) METADATA FROM 'timestamp' VIRTUAL,
            event_time AS TO_TIMESTAMP_LTZ(validated_timestamp(raw_payload, ingested_at), 3),
            WATERMARK FOR event_time AS event_time - {sql_interval(WATERMARK_SECONDS)}
        ) WITH (
            'connector' = 'kafka', 'topic' = '{sql_literal(SOURCE_TOPIC)}',
            'properties.bootstrap.servers' = '{sql_literal(KAFKA_BOOTSTRAP)}',
            'properties.group.id' = '{sql_literal(CONSUMER_GROUP)}',
            'scan.startup.mode' = 'earliest-offset', 'format' = 'raw'
        )
    """)
    create_validation_view(env)


def create_validation_view(env):
    env.execute_sql("""
        CREATE TEMPORARY VIEW validated_events AS
        SELECT r.source_topic, r.source_partition, r.source_offset, r.ingested_at, r.event_time,
               r.raw_payload IS NULL AS payload_is_null,
               CONCAT(r.source_topic, ':', CAST(r.source_partition AS STRING), ':',
                      CAST(r.source_offset AS STRING)) AS record_id, p.*
        FROM raw_events AS r,
        LATERAL TABLE(parse_event(r.raw_payload, r.ingested_at)) AS p(
            event_id, user_id, event_type, event_timestamp_ms, amount, currency,
            category, error_reason, validation_errors, raw_payload_base64)
    """)


def create_business_views(env):
    env.execute_sql('''
        CREATE TEMPORARY VIEW valid_business_events AS
        SELECT event_id, event_type, currency, amount, event_time
        FROM validated_events WHERE error_reason IS NULL AND event_time IS NOT NULL
    ''')
    env.execute_sql(f'''
        CREATE TEMPORARY VIEW unique_window_events AS
        SELECT event_id, event_type, currency, amount, window_time
        FROM (
            SELECT *, ROW_NUMBER() OVER (
                PARTITION BY window_start, window_end, event_id
                ORDER BY event_time ASC) AS row_num
            FROM TABLE(TUMBLE(TABLE valid_business_events,
                       DESCRIPTOR(event_time), {sql_interval(WINDOW_SECONDS)}))
        ) WHERE row_num = 1
    ''')


def business_window_query(payment_only=False):
    # Cascading windows use window_time, preserving the event-time attribute.
    # The second window finalizes the deduplicated output with bounded state.
    columns = ('currency, COUNT(*) AS payment_count, SUM(amount) AS revenue'
               if payment_only else 'event_type, currency, COUNT(*) AS event_count')
    group = 'currency' if payment_only else 'event_type, currency'
    where = "WHERE event_type = 'payment'" if payment_only else ''
    return f'''
        SELECT CAST(window_start AS TIMESTAMP(3)) AS window_start,
               CAST(window_end AS TIMESTAMP(3)) AS window_end, {columns}
        FROM TABLE(TUMBLE(TABLE unique_window_events,
                   DESCRIPTOR(window_time), {sql_interval(WINDOW_SECONDS)}))
        {where}
        GROUP BY window_start, window_end, {group}
    '''


def create_business_sinks(env):
    for table, columns, key in [
        ('payment_revenue_windows', 'currency STRING, payment_count BIGINT, revenue DECIMAL(38, 2)',
         'window_start, window_end, currency'),
        ('activity_windows', 'event_type STRING, currency STRING, event_count BIGINT',
         'window_start, window_end, event_type, currency'),
    ]:
        env.execute_sql(f'''
            CREATE TABLE {table} (
                window_start TIMESTAMP(3), window_end TIMESTAMP(3), {columns},
                PRIMARY KEY ({key}) NOT ENFORCED
            ) WITH (
                'connector' = 'jdbc', 'url' = '{sql_literal(POSTGRES_URL)}',
                'table-name' = '{table}', 'username' = '{sql_literal(POSTGRES_USER)}',
                'password' = '{sql_literal(POSTGRES_PASS)}', 'driver' = 'org.postgresql.Driver',
                'sink.buffer-flush.interval' = '1 s', 'sink.max-retries' = '3'
            )
        ''')


def create_postgres_sink(env):
    env.execute_sql(f"""
        CREATE TABLE processed_events (
            event_id STRING NOT NULL, user_id STRING, event_type STRING,
            event_timestamp_ms BIGINT, amount DECIMAL(10, 2), currency STRING,
            category STRING, ingested_at TIMESTAMP(3), processed_at TIMESTAMP(3),
            source_topic STRING, source_partition INT, source_offset BIGINT,
            PRIMARY KEY (event_id) NOT ENFORCED
        ) WITH (
            'connector' = 'jdbc', 'url' = '{sql_literal(POSTGRES_URL)}',
            'table-name' = '{POSTGRES_TABLE}', 'username' = '{sql_literal(POSTGRES_USER)}',
            'password' = '{sql_literal(POSTGRES_PASS)}', 'driver' = 'org.postgresql.Driver',
            'sink.buffer-flush.max-rows' = '100', 'sink.buffer-flush.interval' = '1 s',
            'sink.max-retries' = '3'
        )
    """)


def create_dlq_sink(env):
    env.execute_sql(f"""
        CREATE TABLE dlq_events (
            record_id STRING, source_topic STRING, source_partition INT,
            source_offset BIGINT, ingested_at TIMESTAMP_LTZ(3),
            raw_payload_base64 STRING, error_reason STRING, validation_errors STRING
        ) WITH (
            'connector' = 'kafka', 'topic' = '{sql_literal(DLQ_TOPIC)}',
            'properties.bootstrap.servers' = '{sql_literal(KAFKA_BOOTSTRAP)}',
            'sink.delivery-guarantee' = 'at-least-once', 'format' = 'json'
        )
    """)


def build_statement_set(env):
    statements = env.create_statement_set()
    statements.add_insert_sql("""
        INSERT INTO processed_events
        SELECT event_id, user_id, event_type, event_timestamp_ms, amount, currency,
               category, CAST(ingested_at AS TIMESTAMP(3)),
               CAST(CURRENT_TIMESTAMP AS TIMESTAMP(3)), source_topic, source_partition, source_offset
        FROM validated_events WHERE error_reason IS NULL
    """)
    statements.add_insert_sql("""
        INSERT INTO dlq_events
        SELECT record_id, source_topic, source_partition, source_offset, ingested_at,
               raw_payload_base64, error_reason, validation_errors
        FROM validated_events WHERE error_reason IS NOT NULL
    """)
    statements.add_insert_sql('INSERT INTO payment_revenue_windows ' + business_window_query(True))
    statements.add_insert_sql('INSERT INTO activity_windows ' + business_window_query())
    if lakehouse_config.enabled():
        lakehouse_config.add_inserts(statements, business_window_query(True))
    return statements


def build_and_run(env):
    result = build_statement_set(env).execute()
    client = result.get_job_client()
    logger.info('Job ID: %s', client.get_job_id())
    try:
        client.get_job_execution_result().result()
    except KeyboardInterrupt:
        logger.info('Cancelling job; retained checkpoints can be restored explicitly.')
        client.cancel().result()


def main():
    ensure_prometheus_reporter_plugin()
    env = create_table_env()
    create_source_table(env)
    create_postgres_sink(env)
    create_dlq_sink(env)
    create_business_views(env)
    create_business_sinks(env)
    if lakehouse_config.enabled():
        lakehouse_config.create_catalog(env)
        lakehouse_config.create_sinks(env)
    # Cluster deployment runs a separate collector so resubmissions cannot bind
    # the same metrics port or interrupt observation when the client exits.
    if os.getenv('APPLICATION_METRICS_ENABLED', '1') == '1':
        start_metrics_server(PG_CONN_PARAMS, KAFKA_BOOTSTRAP, DLQ_TOPIC)
    build_and_run(env)


if __name__ == '__main__':
    main()
