"""PostgreSQL owns durable behavior; processors never write summary snapshots."""
from datetime import datetime, timezone
import hashlib
import json

import psycopg2
from psycopg2.extras import RealDictCursor
from jsonschema import Draft202012Validator

from .auth import identifier
from .config import Settings, EVENT_TOPIC
from .contracts import FORMAT_CHECKER, decode_json, load_schema, validation_errors

DEDUP_SECONDS = 7 * 24 * 3600
VALIDATOR = Draft202012Validator(load_schema(), format_checker=FORMAT_CHECKER)


def fingerprint(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(',', ':'),
                                     ensure_ascii=False).encode()).hexdigest()


def timestamp(value):
    if not isinstance(value, str) or not FORMAT_CHECKER.conforms(value, 'date-time'):
        raise ValueError('Invalid timestamp')
    return datetime.fromisoformat(value.upper().replace('Z', '+00:00')).astimezone(timezone.utc)


def decode_transport(payload, key):
    envelope = decode_json(payload)
    if (not isinstance(envelope, dict) or type(envelope.get('transport_version')) is not int
            or envelope['transport_version'] != 1
            or not identifier(envelope.get('source_id'))
            or not isinstance(envelope.get('request_id'), str)
            or not envelope['request_id'] or len(envelope['request_id']) > 200):
        raise ValueError('Invalid transport envelope')
    event = envelope.get('event')
    if validation_errors(event, VALIDATOR):
        raise ValueError('Invalid event contract')
    if key != event['user_id']:
        raise ValueError('Kafka key must match event user')
    return envelope, timestamp(event['occurred_at']), timestamp(envelope.get('received_at'))


class ProfileStore:
    def __init__(self, settings=None, connect=None):
        self.settings = settings or Settings.from_environment()
        self.connect = connect or (lambda: psycopg2.connect(**self.settings.database_parameters()))

    def apply(self, topic, partition, offset, key, payload):
        """Commit receipt + event identity + order identity + summary atomically.

        A lost commit acknowledgement raises and is retried by Flink. A committed
        receipt makes that retry a no-op, even after event-ID retention expires.
        """
        if topic != EVENT_TOPIC or partition < 0 or offset < 0:
            raise ValueError('Invalid Kafka coordinates')
        # Distinguish tombstones from empty values and receipt invalid Kafka records too.
        raw = b'\x00' if payload is None else b'\x01' + (payload.encode() if isinstance(payload, str) else payload)
        raw_hash = hashlib.sha256(raw).hexdigest()
        try:
            envelope, occurred, received = decode_transport(payload, key)
        except (ValueError, TypeError, UnicodeError, OverflowError, RecursionError):
            envelope = None
        connection = self.connect()
        try:
            with connection, connection.cursor() as cursor:
                cursor.execute("SET LOCAL statement_timeout = '15s'")
                cursor.execute("SET LOCAL lock_timeout = '10s'")
                cursor.execute("""INSERT INTO journeypulse.kafka_receipts
                    (topic, partition_id, offset_id, payload_hash, outcome)
                    VALUES (%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING RETURNING offset_id""",
                    (topic, partition, offset, raw_hash, 'invalid' if envelope is None else 'applied'))
                if cursor.fetchone() is None:
                    cursor.execute("""SELECT payload_hash FROM journeypulse.kafka_receipts
                        WHERE topic=%s AND partition_id=%s AND offset_id=%s""", (topic, partition, offset))
                    if cursor.fetchone()[0] != raw_hash:
                        raise ValueError('Kafka coordinate reused: topic recreation requires a new ledger namespace')
                    return 'replay'
                if envelope is None:
                    return 'invalid'
                event = envelope['event']
                # Serialize global event-ID decisions, including expiry/replacement.
                cursor.execute('SELECT pg_advisory_xact_lock(hashtextextended(%s, 712340002))',
                               (event['event_id'],))
                event_hash = fingerprint(dict(source_id=envelope['source_id'], event=event))
                cursor.execute('DELETE FROM journeypulse.event_ids WHERE event_id=%s AND expires_at <= clock_timestamp()',
                               (event['event_id'],))
                cursor.execute("""INSERT INTO journeypulse.event_ids VALUES
                    (%s,%s,clock_timestamp() + %s * interval '1 second')
                    ON CONFLICT DO NOTHING RETURNING event_id""", (event['event_id'], event_hash, DEDUP_SECONDS))
                if cursor.fetchone() is None:
                    cursor.execute('SELECT payload_hash FROM journeypulse.event_ids WHERE event_id=%s', (event['event_id'],))
                    outcome = 'duplicate' if cursor.fetchone()[0] == event_hash else 'conflict'
                    cursor.execute("""UPDATE journeypulse.kafka_receipts SET outcome=%s
                        WHERE topic=%s AND partition_id=%s AND offset_id=%s""", (outcome, topic, partition, offset))
                    return outcome
                user = event['user_id']
                cursor.execute('INSERT INTO journeypulse.customers(user_id) VALUES (%s) ON CONFLICT DO NOTHING', (user,))
                cursor.execute('INSERT INTO journeypulse.behavioral_summaries(user_id) VALUES (%s) ON CONFLICT DO NOTHING', (user,))
                # Lock behavior, not attributes; the processor needs no customer UPDATE privilege.
                cursor.execute('SELECT user_id FROM journeypulse.behavioral_summaries WHERE user_id=%s FOR UPDATE', (user,))
                order_delta = 0
                order_time = None
                if event['event_name'] == 'order_completed':
                    props = event['properties']
                    cursor.execute("""INSERT INTO journeypulse.completed_orders
                        (user_id,order_id,occurred_at,restaurant_id,cart_id) VALUES (%s,%s,%s,%s,%s)
                        ON CONFLICT DO NOTHING RETURNING order_id""",
                        (user, props['order_id'], occurred, props['restaurant_id'], props['cart_id']))
                    order_delta = int(cursor.fetchone() is not None)
                    order_time = occurred if order_delta else None
                # Increments are gated by durable unique receipts AND bounded event IDs.
                # They cannot be repeated by checkpoint replay; no Flink snapshot exists.
                cursor.execute("""UPDATE journeypulse.behavioral_summaries SET
                    event_counts=jsonb_set(event_counts, ARRAY[%s],
                        to_jsonb(COALESCE((event_counts->>%s)::bigint,0)+1)),
                    completed_orders=completed_orders+%s,
                    latest_activity_at=GREATEST(latest_activity_at,%s),
                    latest_order_at=GREATEST(latest_order_at,%s),
                    latest_received_at=GREATEST(latest_received_at,%s),
                    processed_at=clock_timestamp(), revision=revision+1 WHERE user_id=%s""",
                    (event['event_name'], event['event_name'], order_delta, occurred, order_time, received, user))
                return 'applied'
        finally:
            connection.close()

    def prune_event_ids(self):
        """Scheduled maintenance bounds the index; receipts and orders stay durable."""
        connection = self.connect()
        try:
            with connection, connection.cursor() as cursor:
                cursor.execute('DELETE FROM journeypulse.event_ids WHERE expires_at <= clock_timestamp()')
                return cursor.rowcount
        finally:
            connection.close()

    def update(self, user_id, attributes, onboarding=False):
        connection = self.connect()
        try:
            with connection, connection.cursor() as cursor:
                if onboarding:
                    cursor.execute('INSERT INTO journeypulse.customers(user_id) VALUES (%s) ON CONFLICT DO NOTHING', (user_id,))
                assignments = ['attributes_updated_at=clock_timestamp()']
                parameters = []
                for name in ('preferred_language', 'marketing_consent'):
                    if name in attributes:
                        assignments.append(name + '=%s')
                        parameters.append(attributes[name])
                if 'marketing_consent' in attributes:
                    assignments.append('consent_updated_at=clock_timestamp()')
                if onboarding:
                    assignments.append('onboarded_at=COALESCE(onboarded_at,clock_timestamp())')
                cursor.execute('UPDATE journeypulse.customers SET ' + ','.join(assignments) +
                               ' WHERE user_id=%s RETURNING user_id', (*parameters, user_id))
                return cursor.fetchone() is not None
        finally:
            connection.close()

    def get(self, user_id):
        connection = self.connect()
        try:
            with connection, connection.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute("""SELECT c.*, b.event_counts, b.completed_orders,
                    b.latest_activity_at,b.latest_order_at,b.latest_received_at,b.processed_at,b.revision,
                    clock_timestamp() AS served_at FROM journeypulse.customers c
                    LEFT JOIN journeypulse.behavioral_summaries b USING(user_id) WHERE user_id=%s""", (user_id,))
                row = cursor.fetchone()
                return dict(row) if row else None
        finally:
            connection.close()

    def ready(self):
        connection = self.connect()
        try:
            with connection, connection.cursor() as cursor:
                cursor.execute('SELECT version FROM journeypulse.schema_migrations WHERE version=2')
                return cursor.fetchone() is not None
        finally:
            connection.close()
