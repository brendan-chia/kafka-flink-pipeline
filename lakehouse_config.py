"""Optional Iceberg SQL integration; no change to the default four-output job."""
import os
import re
from pathlib import Path

ICEBERG_VERSION = '1.12.0'
JARS = (f'iceberg-flink-runtime-2.2-{ICEBERG_VERSION}.jar',
        f'iceberg-aws-bundle-{ICEBERG_VERSION}.jar',
        'hadoop-client-api-3.3.6.jar', 'hadoop-client-runtime-3.3.6.jar', 'commons-logging-1.2.jar')


def enabled():
    value = os.getenv('LAKEHOUSE_ENABLED', '0')
    if value not in ('0', '1'):
        raise ValueError('LAKEHOUSE_ENABLED must be 0 or 1')
    return value == '1'


def configure_gateway_classpath(root):
    # Hadoop classes must be visible to the JVM's application classloader;
    # pipeline.jars alone cannot satisfy Iceberg's Hadoop configuration lookup.
    if enabled():
        jars = [str(Path(root) / 'jars' / name) for name in JARS
                if name.startswith(('hadoop-', 'commons-logging'))]
        existing = os.getenv('HADOOP_CLASSPATH')
        os.environ['HADOOP_CLASSPATH'] = os.pathsep.join(([existing] if existing else []) + jars)


def namespace():
    value = os.getenv('LAKEHOUSE_NAMESPACE', 'analytics')
    if not re.fullmatch(r'[a-z][a-z0-9_]{0,62}', value):
        raise ValueError('LAKEHOUSE_NAMESPACE must be a lowercase SQL identifier')
    return value


def properties_sql(properties):
    def quote(value):
        return "'" + str(value).replace("'", "''") + "'"
    return ', '.join(f'{quote(key)}={quote(value)}' for key, value in properties.items())


def create_catalog(env):
    properties = {
        'type': 'iceberg', 'catalog-type': 'rest',
        'uri': os.getenv('ICEBERG_REST_URI', 'http://localhost:18181'),
        'warehouse': 's3://warehouse/',
        'io-impl': 'org.apache.iceberg.aws.s3.S3FileIO',
        's3.endpoint': os.getenv('ICEBERG_S3_ENDPOINT', 'http://localhost:19000'),
        's3.path-style-access': 'true', 'client.region': 'us-east-1',
        's3.access-key-id': os.getenv('ICEBERG_ACCESS_KEY', 'lakehouse'),
        's3.secret-access-key': os.getenv('ICEBERG_SECRET_KEY', 'lakehouse-local-password'),
        'rest.auth.type': 'none',
    }
    env.execute_sql(f'CREATE CATALOG lakehouse WITH ({properties_sql(properties)})')
    env.execute_sql(f'CREATE DATABASE IF NOT EXISTS lakehouse.{namespace()}')


def create_sinks(env):
    prefix = f'lakehouse.{namespace()}'
    tables = [
        ('event_history', '''record_id STRING, source_topic STRING, source_partition INT,
            source_offset BIGINT, ingested_at TIMESTAMP(3), ingest_date DATE,
            archived_at TIMESTAMP(3), raw_payload_base64 STRING, payload_is_null BOOLEAN,
            error_reason STRING, validation_errors STRING, validator_version STRING''', 'ingest_date'),
        ('validated_events', '''record_id STRING, event_id STRING, user_id STRING,
            event_type STRING, event_timestamp_ms BIGINT, amount DECIMAL(10,2),
            currency STRING, category STRING, event_date DATE, ingested_at TIMESTAMP(3),
            archived_at TIMESTAMP(3), source_topic STRING, source_partition INT,
            source_offset BIGINT''', 'event_date'),
        ('revenue_finalized', '''window_start TIMESTAMP(3), window_end TIMESTAMP(3),
            currency STRING, payment_count BIGINT, revenue DECIMAL(38,2),
            window_date DATE, archived_at TIMESTAMP(3)''', 'window_date'),
    ]
    for name, columns, partition in tables:
        env.execute_sql(f'''CREATE TABLE IF NOT EXISTS {prefix}.{name} ({columns})
            PARTITIONED BY ({partition}) WITH (
                'format-version'='2', 'write.format.default'='parquet',
                'write.target-file-size-bytes'='134217728',
                'write.parquet.compression-codec'='snappy')''')


def add_inserts(statements, revenue_query):
    prefix = f'lakehouse.{namespace()}'
    statements.add_insert_sql(f'''INSERT INTO {prefix}.event_history
        SELECT record_id, source_topic, source_partition, source_offset,
            CAST(ingested_at AS TIMESTAMP(3)), CAST(ingested_at AS DATE),
            CAST(CURRENT_TIMESTAMP AS TIMESTAMP(3)), raw_payload_base64,
            payload_is_null, error_reason, validation_errors, 'payments-v1'
        FROM validated_events''')
    statements.add_insert_sql(f'''INSERT INTO {prefix}.validated_events
        SELECT record_id, event_id, user_id, event_type, event_timestamp_ms,
            amount, currency, category, CAST(TO_TIMESTAMP_LTZ(event_timestamp_ms,3) AS DATE),
            CAST(ingested_at AS TIMESTAMP(3)), CAST(CURRENT_TIMESTAMP AS TIMESTAMP(3)),
            source_topic, source_partition, source_offset
        FROM validated_events WHERE error_reason IS NULL''')
    statements.add_insert_sql(f'''INSERT INTO {prefix}.revenue_finalized
        SELECT window_start, window_end, currency, payment_count, revenue,
            CAST(window_start AS DATE), CAST(CURRENT_TIMESTAMP AS TIMESTAMP(3))
        FROM ({revenue_query})''')
