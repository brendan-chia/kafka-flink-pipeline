# monitoring/metrics.py
"""
Pipeline observability — Prometheus metrics pushed to Pushgateway.

Because the pipeline uses PyFlink Table API + StatementSet (pure SQL execution),
we cannot instrument inside map/process functions. Instead, a background thread
polls Postgres row counts and Kafka DLQ lag to derive metrics externally.
"""

import threading
import time
import logging

import psycopg2
from kafka import KafkaConsumer, TopicPartition
from prometheus_client import CollectorRegistry, Counter, Gauge, push_to_gateway

logger = logging.getLogger(__name__)

PUSHGATEWAY_URL    = "localhost:9091"
POLL_INTERVAL_SEC  = 10   # how often to poll & push

registry = CollectorRegistry()

# ── Metric definitions ─────────────────────────────────────────────────────────

events_processed_total = Gauge(
    'flink_events_processed_total',
    'Total valid events written to PostgreSQL (cumulative row count)',
    registry=registry
)

dlq_events_total = Gauge(
    'flink_dlq_events_total',
    'Total invalid events in the DLQ topic (end offset)',
    registry=registry
)

dlq_rate = Gauge(
    'flink_dlq_rate',
    'DLQ events as a fraction of total events (0.0 to 1.0)',
    registry=registry
)

consumer_lag = Gauge(
    'flink_consumer_lag_messages',
    'Kafka consumer lag: latest offset minus committed offset',
    registry=registry
)


# ── Metric collectors ──────────────────────────────────────────────────────────

def _get_processed_count(pg_conn_params: dict) -> int:
    """Query Postgres for the current row count of processed_events."""
    try:
        conn = psycopg2.connect(**pg_conn_params)
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM processed_events;")
        count = cur.fetchone()[0]
        cur.close()
        conn.close()
        return count
    except Exception as e:
        logger.warning(f"[metrics] Postgres query failed: {e}")
        return -1


def _get_dlq_end_offset(bootstrap_servers: str, dlq_topic: str) -> int:
    """
    Get total messages in DLQ by summing end offsets across all partitions.
    End offset = total messages ever written (since offset 0), which is what we want.
    """
    try:
        consumer = KafkaConsumer(bootstrap_servers=bootstrap_servers)
        partitions = consumer.partitions_for_topic(dlq_topic) or set()
        tps = [TopicPartition(dlq_topic, p) for p in partitions]
        if not tps:
            consumer.close()
            return 0
        end_offsets = consumer.end_offsets(tps)
        total = sum(end_offsets.values())
        consumer.close()
        return total
    except Exception as e:
        logger.warning(f"[metrics] DLQ offset query failed: {e}")
        return -1


def _get_consumer_lag(
    bootstrap_servers: str,
    source_topic: str,
    consumer_group: str
) -> int:
    """
    Lag = sum over partitions of (end_offset - committed_offset).
    Uses a temporary consumer to read committed offsets for the group.
    """
    try:
        from kafka import KafkaAdminClient
        from kafka.structs import OffsetAndMetadata

        consumer = KafkaConsumer(
            bootstrap_servers=bootstrap_servers,
            group_id=consumer_group,
            enable_auto_commit=False
        )
        partitions = consumer.partitions_for_topic(source_topic) or set()
        tps = [TopicPartition(source_topic, p) for p in partitions]
        if not tps:
            consumer.close()
            return 0

        end_offsets   = consumer.end_offsets(tps)
        committed      = {tp: consumer.committed(tp) or 0 for tp in tps}
        total_lag      = sum(end_offsets[tp] - committed[tp] for tp in tps)

        consumer.close()
        return max(total_lag, 0)
    except Exception as e:
        logger.warning(f"[metrics] Consumer lag query failed: {e}")
        return -1


# ── Background pusher ──────────────────────────────────────────────────────────

def start_metrics_pusher(
    pg_conn_params: dict,
    bootstrap_servers: str,
    source_topic: str,
    dlq_topic: str,
    consumer_group: str,
):
    """
    Spawns a daemon thread that polls metrics every POLL_INTERVAL_SEC seconds
    and pushes them to the Pushgateway. Call this once before stmt_set.execute().
    """
    def _loop():
        logger.info("[metrics] Background metrics pusher started.")
        while True:
            processed = _get_processed_count(pg_conn_params)
            dlq       = _get_dlq_end_offset(bootstrap_servers, dlq_topic)

            if processed >= 0:
                events_processed_total.set(processed)
            if dlq >= 0:
                dlq_events_total.set(dlq)

            # DLQ rate: dlq / (processed + dlq), guard against divide-by-zero
            total = (processed if processed >= 0 else 0) + (dlq if dlq >= 0 else 0)
            if total > 0:
                dlq_rate.set(dlq / total)

            lag = _get_consumer_lag(bootstrap_servers, source_topic, consumer_group)
            if lag >= 0:
                consumer_lag.set(lag)

            try:
                push_to_gateway(PUSHGATEWAY_URL, job='flink_pipeline', registry=registry)
                logger.debug(f"[metrics] Pushed — processed={processed}, dlq={dlq}, lag={lag}")
            except Exception as e:
                logger.warning(f"[metrics] Push failed: {e}")

            time.sleep(POLL_INTERVAL_SEC)

    t = threading.Thread(target=_loop, daemon=True)
    t.start()