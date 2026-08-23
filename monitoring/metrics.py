# monitoring/metrics.py
"""Application-level pipeline metrics exposed for Prometheus to scrape.

Flink's native runtime metrics are exposed separately by its Prometheus metric
reporter. This module owns only business and data-quality metrics that are
derived from PostgreSQL and Kafka.
"""

import threading
import time
import logging
import os

import psycopg2
from kafka import KafkaConsumer, TopicPartition
from prometheus_client import CollectorRegistry, Gauge, start_http_server

logger = logging.getLogger(__name__)

METRICS_HOST = os.getenv("APPLICATION_METRICS_HOST", "0.0.0.0")
METRICS_PORT = int(os.getenv("APPLICATION_METRICS_PORT", "8000"))
POLL_INTERVAL_SEC = 10

registry = CollectorRegistry()

# ── Metric definitions ─────────────────────────────────────────────────────────

events_processed_total = Gauge(
    'pipeline_events_processed_total',
    'Total valid events written to PostgreSQL (cumulative row count)',
    registry=registry
)

dlq_events_total = Gauge(
    'pipeline_dlq_events_total',
    'Total invalid events in the DLQ topic (end offset)',
    registry=registry
)

dlq_rate = Gauge(
    'pipeline_dlq_rate',
    'DLQ events as a fraction of total events (0.0 to 1.0)',
    registry=registry
)

collection_success = Gauge(
    'pipeline_metrics_collection_success',
    'Whether the latest collection succeeded (1) or failed (0)',
    ['source'],
    registry=registry
)

last_collection_timestamp = Gauge(
    'pipeline_metrics_last_collection_timestamp_seconds',
    'Unix timestamp of the latest successful metric collection',
    ['source'],
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


# ── HTTP metrics endpoint and background collector ────────────────────────────

def start_metrics_server(
    pg_conn_params: dict,
    bootstrap_servers: str,
    dlq_topic: str,
):
    """Expose ``/metrics`` and refresh derived metrics in a daemon thread."""
    start_http_server(METRICS_PORT, addr=METRICS_HOST, registry=registry)
    logger.info(
        "[metrics] Application metrics available at http://%s:%s/metrics",
        METRICS_HOST,
        METRICS_PORT,
    )

    def _loop():
        logger.info("[metrics] Background business-metrics collector started.")
        while True:
            processed = _get_processed_count(pg_conn_params)
            dlq = _get_dlq_end_offset(bootstrap_servers, dlq_topic)
            now = time.time()

            if processed >= 0:
                events_processed_total.set(processed)
                collection_success.labels(source="postgres").set(1)
                last_collection_timestamp.labels(source="postgres").set(now)
            else:
                collection_success.labels(source="postgres").set(0)

            if dlq >= 0:
                dlq_events_total.set(dlq)
                collection_success.labels(source="kafka_dlq").set(1)
                last_collection_timestamp.labels(source="kafka_dlq").set(now)
            else:
                collection_success.labels(source="kafka_dlq").set(0)

            if processed >= 0 and dlq >= 0 and processed + dlq > 0:
                total = processed + dlq
                dlq_rate.set(dlq / total)

            logger.debug(
                "[metrics] Refreshed processed=%s, dlq=%s",
                processed,
                dlq,
            )

            time.sleep(POLL_INTERVAL_SEC)

    t = threading.Thread(target=_loop, daemon=True)
    t.start()
