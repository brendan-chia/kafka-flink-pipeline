"""Repeatable local setup; never changes existing topic settings or contract versions."""
import time

from kafka.admin import KafkaAdminClient, NewTopic, ConfigResource, ConfigResourceType
from kafka.errors import KafkaError, TopicAlreadyExistsError
import psycopg2
from psycopg2.extras import Json

from .config import Settings, TOPICS, PARTITIONS, REPLICATION_FACTOR, RETENTION_MS
from .contracts import ROOT, EVENT_NAMES, SCHEMA_VERSION, event_schema
from .segments import SegmentSettings, register_rules

TOPIC_CONFIG = {'cleanup.policy': 'delete', 'retention.ms': str(RETENTION_MS),
                'message.timestamp.type': 'LogAppendTime'}


def connect_with_retry(factory, errors, timeout=60):
    deadline = time.monotonic() + timeout
    while True:
        try:
            return factory()
        except errors:
            if time.monotonic() >= deadline:
                raise
            time.sleep(2)


def ensure_topics(admin, create):
    present = set(admin.list_topics())
    for name in TOPICS:
        if name not in present:
            if not create:
                raise ValueError('Missing JourneyPulse topic: ' + name)
            try:
                admin.create_topics([NewTopic(name, PARTITIONS, REPLICATION_FACTOR,
                                              topic_configs=TOPIC_CONFIG)], timeout_ms=10000)
            except TopicAlreadyExistsError:
                pass  # Another bootstrap may have created it; verify below.
    details = admin.describe_topics(list(TOPICS))
    if {topic['topic'] for topic in details} != set(TOPICS):
        raise ValueError('Incomplete JourneyPulse topic metadata response')
    for topic in details:
        partitions = topic['partitions']
        if (topic['error_code'] != 0 or len(partitions) != PARTITIONS
                or any(len(p['replicas']) != REPLICATION_FACTOR or p['leader'] < 0
                       for p in partitions)):
            raise ValueError('JourneyPulse topic topology mismatch: ' + topic['topic'])
    responses = admin.describe_configs([ConfigResource(ConfigResourceType.TOPIC, name)
                                        for name in TOPICS])
    checked = set()
    for response in responses:
        for resource in response.resources:
            error_code, _, _, name, entries = resource
            if error_code != 0:
                raise ValueError('Cannot read JourneyPulse topic settings: ' + name)
            actual = {entry[0]: entry[1] for entry in entries}
            if any(actual.get(key) != value for key, value in TOPIC_CONFIG.items()):
                raise ValueError('JourneyPulse topic settings mismatch: ' + name)
            checked.add(name)
    if checked != set(TOPICS):
        raise ValueError('Incomplete JourneyPulse topic configuration response')


def ensure_database(connection, create):
    with connection:
        with connection.cursor() as cursor:
            if not create:
                cursor.execute("SET TRANSACTION READ ONLY")
            cursor.execute("SET LOCAL lock_timeout = '10s'")
            cursor.execute("SET LOCAL statement_timeout = '10s'")
            if create:
                # Serialize catalogue bootstrap on the shared database.
                cursor.execute("SELECT pg_advisory_xact_lock(712340001)")
                cursor.execute((ROOT / 'journeypulse/sql/001_foundation.sql').read_text(encoding='utf-8'))
            cursor.execute("SELECT version FROM journeypulse.schema_migrations WHERE version = 1")
            if cursor.fetchone() is None:
                raise ValueError('JourneyPulse foundation migration is missing')
            if create:
                cursor.execute((ROOT / 'journeypulse/sql/002_profiles.sql').read_text(encoding='utf-8'))
            cursor.execute('SELECT version FROM journeypulse.schema_migrations WHERE version = 2')
            if cursor.fetchone() is None:
                raise ValueError('JourneyPulse profiles migration is missing; rerun bootstrap')
            if create:
                cursor.execute((ROOT / 'journeypulse/sql/003_segments.sql').read_text(encoding='utf-8'))
            cursor.execute('SELECT version FROM journeypulse.schema_migrations WHERE version = 3')
            if cursor.fetchone() is None:
                raise ValueError('JourneyPulse segments migration is missing; rerun bootstrap')
            register_rules(cursor, SegmentSettings.from_environment(), create)
            for name in EVENT_NAMES:
                definition = event_schema(name)
                if create:
                    cursor.execute(
                        """INSERT INTO journeypulse.event_definitions
                           (event_name, schema_version, definition) VALUES (%s, %s, %s)
                           ON CONFLICT (event_name, schema_version) DO NOTHING""",
                        (name, SCHEMA_VERSION, Json(definition)))
                cursor.execute(
                    """SELECT definition FROM journeypulse.event_definitions
                       WHERE event_name = %s AND schema_version = %s""", (name, SCHEMA_VERSION))
                row = cursor.fetchone()
                if row is None or row[0] != definition:
                    raise ValueError('Missing or changed immutable contract: ' + name + ' v1')


def run(create=False):
    settings = Settings.from_environment()
    admin = connect_with_retry(
        lambda: KafkaAdminClient(bootstrap_servers=settings.kafka_bootstrap,
                                 client_id='journeypulse-foundation',
                                 request_timeout_ms=10000, api_version_auto_timeout_ms=5000),
        (KafkaError,))
    try:
        ensure_topics(admin, create)
    finally:
        admin.close()
    connection = connect_with_retry(lambda: psycopg2.connect(**settings.database_parameters()),
                                    (psycopg2.OperationalError,))
    try:
        ensure_database(connection, create)
    finally:
        connection.close()
    return {'status': 'ready', 'mode': 'bootstrap' if create else 'read-only check',
            'topics': list(TOPICS), 'partitions': PARTITIONS,
            'postgres_schema': 'journeypulse', 'schema_version': SCHEMA_VERSION,
            'database_migration_version': 3,
            'registered_events': list(EVENT_NAMES)}
