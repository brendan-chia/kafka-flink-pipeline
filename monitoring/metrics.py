"""Read-only operational metrics; source observation never commits offsets."""
from collections import Counter, deque
import logging
import os
import threading
import time

import psycopg2
from kafka import KafkaConsumer, TopicPartition
from prometheus_client import CollectorRegistry, Counter as PromCounter, Gauge, start_http_server
from event_contract import decode_event
from datalens import evidence

logger = logging.getLogger(__name__)
METRICS_HOST = os.getenv('APPLICATION_METRICS_HOST', '0.0.0.0')
METRICS_PORT = int(os.getenv('APPLICATION_METRICS_PORT', '8000'))
POLL_INTERVAL_SEC = 10
WINDOW_SECONDS = 300
registry = CollectorRegistry()


def gauge(name, help_text, labels=()):
    return Gauge(name, help_text, labels, registry=registry)


events_processed_total = gauge('pipeline_events_processed_total', 'Unique audit row count; gauge, not a delivery counter')
dlq_events_total = gauge('pipeline_dlq_events_total', 'Sum of DLQ end offsets; includes replay, not retained message count')
dlq_rate = gauge('pipeline_dlq_rate', 'Legacy cumulative fraction; not used for alerts')
collection_success = gauge('pipeline_metrics_collection_success', 'Latest collection succeeded', ['source'])
last_collection_timestamp = gauge('pipeline_metrics_last_collection_timestamp_seconds', 'Last successful collection epoch seconds', ['source'])
freshness = gauge('pipeline_audit_freshness_seconds', 'Age of newest audit processing timestamp; NaN if empty')
payment_freshness = gauge('pipeline_payment_audit_freshness_seconds', 'Age of latest payment audit processing timestamp; NaN if empty')
payment_event_age = gauge('pipeline_payment_event_age_seconds', 'Age of latest audited payment event time; not a completeness guarantee')
payment_recent_rows = gauge('pipeline_payment_audit_recent_rows', 'Unique payment audit rows processed in the last five minutes')
evidence_success = gauge('pipeline_evidence_collection_success', 'Latest DataLens evidence persistence succeeded', ['check'])
recent_rows = gauge('pipeline_audit_recent_rows', 'Audit rows processed in the last five minutes')
latency = gauge('pipeline_audit_latency_seconds', 'Recent latency percentile excluding negative durations', ['clock', 'quantile'])
observed = PromCounter('pipeline_input_events_observed_total', 'Source records classified by latest-offset observer', ['outcome'], registry=registry)
window_records = gauge('pipeline_input_events_window', 'Observed records with broker timestamps in the last five minutes', ['outcome'])
invalid_fraction = gauge('pipeline_invalid_fraction_window', 'Invalid source fraction over five minutes; NaN if empty')
invalid_reasons = gauge('pipeline_invalid_events_window', 'Invalid source records by bounded primary reason over five minutes', ['reason'])
coverage_start = gauge('pipeline_observer_coverage_start_timestamp_seconds', 'Latest observer start/reset; windows are partial for five minutes')
REASONS = ('UNSUPPORTED_SCHEMA_VERSION', 'NULL_PAYLOAD', 'MALFORMED_JSON', 'INVALID_JSON_OBJECT', 'INVALID_EVENT_ID',
           'INVALID_USER_ID', 'INVALID_EVENT_TYPE', 'INVALID_TIMESTAMP', 'FUTURE_EVENT_TIME',
           'INVALID_CURRENCY', 'INVALID_AMOUNT', 'INVALID_AMOUNT_PRECISION', 'MULTIPLE_ERRORS', 'OTHER')


class QualityWindow:
    def __init__(self):
        self.records = deque()

    def add(self, timestamp, reason):
        self.records.append((timestamp, reason))

    def snapshot(self, now):
        # Cross-partition arrival order need not be timestamp order.
        self.records = deque((ts, reason) for ts, reason in self.records
                             if now - WINDOW_SECONDS < ts <= now)
        reasons = Counter(reason for _, reason in self.records if reason)
        invalid = sum(reasons.values())
        return len(self.records) - invalid, invalid, reasons

    def expose(self, now):
        valid, invalid, reasons = self.snapshot(now)
        window_records.labels(outcome='valid').set(valid)
        window_records.labels(outcome='invalid').set(invalid)
        invalid_fraction.set(invalid / (valid + invalid) if valid + invalid else float('nan'))
        for reason in REASONS:
            invalid_reasons.labels(reason=reason).set(reasons[reason])


def collect_postgres(params):
    conn = psycopg2.connect(**dict(params, connect_timeout=5, options='-c statement_timeout=5000 -c timezone=UTC'))
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*), EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP AT TIME ZONE 'UTC' - MAX(processed_at))) FROM processed_events")
            count, age = cur.fetchone()
            cur.execute('''SELECT COUNT(*),
                percentile_cont(ARRAY[0.5,0.95,0.99]) WITHIN GROUP (ORDER BY
                    EXTRACT(EPOCH FROM processed_at) - event_timestamp_ms / 1000.0)
                    FILTER (WHERE EXTRACT(EPOCH FROM processed_at) >= event_timestamp_ms / 1000.0),
                percentile_cont(ARRAY[0.5,0.95,0.99]) WITHIN GROUP (ORDER BY
                    EXTRACT(EPOCH FROM (processed_at - ingested_at)))
                    FILTER (WHERE processed_at >= ingested_at)
                FROM processed_events
                WHERE processed_at >= CURRENT_TIMESTAMP AT TIME ZONE 'UTC' - INTERVAL '5 minutes' ''')
            recent, event_latency, broker_latency = cur.fetchone()
        events_processed_total.set(count)
        freshness.set(float(age) if age is not None else float('nan'))
        recent_rows.set(recent)
        for clock, values in [('event', event_latency), ('broker', broker_latency)]:
            for i, quantile in enumerate(('0.5', '0.95', '0.99')):
                latency.labels(clock=clock, quantile=quantile).set(values[i] if values else float('nan'))
        return count
    finally:
        conn.close()


def collect_payment_freshness(params):
    row = evidence.payment_freshness(params)
    for metric, key in ((payment_freshness, 'processing_age_seconds'),
                        (payment_event_age, 'event_age_seconds')):
        metric.set(float(row[key]) if row[key] is not None else float('nan'))
    payment_recent_rows.set(row['recent_payment_count'])
    return row


def try_persist(check, function, *args):
    # Persistence failure must not reset the source observer or obscure metrics.
    try:
        function(*args)
        evidence_success.labels(check=check).set(1)
    except Exception:
        evidence_success.labels(check=check).set(0)
        logger.warning('DataLens evidence persistence failed for %s; apply migration 004', check, exc_info=True)


def new_consumer(bootstrap):
    return KafkaConsumer(bootstrap_servers=bootstrap, group_id=None, enable_auto_commit=False,
                         request_timeout_ms=10000, api_version_auto_timeout_ms=5000)


def dlq_offset(bootstrap, topic):
    consumer = new_consumer(bootstrap)
    try:
        partitions = consumer.partitions_for_topic(topic)
        if not partitions:
            raise RuntimeError('DLQ topic has no available partitions')
        return sum(consumer.end_offsets([TopicPartition(topic, p) for p in partitions]).values())
    finally:
        consumer.close()


def mark(source, success):
    collection_success.labels(source=source).set(int(success))
    if success:
        last_collection_timestamp.labels(source=source).set(time.time())


def start_metrics_server(pg_conn_params, bootstrap_servers, dlq_topic):
    start_http_server(METRICS_PORT, addr=METRICS_HOST, registry=registry)

    persist_evidence = os.getenv('DATALENS_EVIDENCE_ENABLED', '0') == '1'

    def database_loop():
        while True:
            try:
                collect_postgres(pg_conn_params)
                mark('postgres', True)
                row = collect_payment_freshness(pg_conn_params)
                if persist_evidence:
                    try_persist('payment_freshness', evidence.record_payment_freshness, pg_conn_params, row)
            except Exception:
                mark('postgres', False)
                logger.warning('PostgreSQL metrics collection failed', exc_info=True)
            time.sleep(POLL_INTERVAL_SEC)

    def kafka_loop():
        consumer = None
        quality = QualityWindow()
        source_topic = os.getenv('SOURCE_TOPIC', 'user-events')
        skew = int(os.getenv('EVENT_MAX_FUTURE_SKEW_SECONDS', '60')) * 1000
        last_dlq_poll = 0
        last_evidence_poll = 0
        observer_start = None
        while True:
            try:
                if consumer is None:
                    consumer = new_consumer(bootstrap_servers)
                    partitions = consumer.partitions_for_topic(source_topic)
                    if not partitions:
                        raise RuntimeError('Source topic has no available partitions')
                    consumer.assign([TopicPartition(source_topic, p) for p in partitions])
                    consumer.seek_to_end()
                    for tp in consumer.assignment():
                        consumer.position(tp)  # Resolve lazy seeks before coverage starts.
                    observer_start = time.time()
                    coverage_start.set(observer_start)
                    quality = QualityWindow()
                for records in consumer.poll(timeout_ms=1000, max_records=5000).values():
                    for record in records:
                        reason = decode_event(record.value, record.timestamp, skew)[7]
                        if reason and reason not in REASONS:
                            reason = 'OTHER'
                        quality.add(record.timestamp / 1000, reason)
                        observed.labels(outcome='invalid' if reason else 'valid').inc()
                quality.expose(time.time())
                mark('kafka_source', True)
                if persist_evidence and time.monotonic() - last_evidence_poll >= POLL_INTERVAL_SEC:
                    now = time.time()
                    valid, invalid, reasons = quality.snapshot(now)
                    try_persist('source_validation_window', evidence.record_source_quality,
                        pg_conn_params, valid, invalid, reasons, now, observer_start,
                        {'source_topic': source_topic, 'max_future_skew_seconds': skew / 1000,
                         'observer_window_seconds': WINDOW_SECONDS})
                    last_evidence_poll = time.monotonic()
            except Exception:
                mark('kafka_source', False)
                logger.warning('Source observer failed; coverage will reset', exc_info=True)
                if consumer is not None:
                    consumer.close()
                    consumer = None
                time.sleep(POLL_INTERVAL_SEC)
            if time.monotonic() - last_dlq_poll >= POLL_INTERVAL_SEC:
                try:
                    dlq = dlq_offset(bootstrap_servers, dlq_topic)
                    dlq_events_total.set(dlq)
                    processed = events_processed_total._value.get()
                    dlq_rate.set(dlq / (processed + dlq) if processed + dlq else float('nan'))
                    mark('kafka_dlq', True)
                except Exception:
                    mark('kafka_dlq', False)
                    logger.warning('DLQ offset collection failed', exc_info=True)
                last_dlq_poll = time.monotonic()

    for source in ('postgres', 'kafka_source', 'kafka_dlq'):
        mark(source, False)
    for loop in (database_loop, kafka_loop):
        threading.Thread(target=loop, daemon=True).start()
