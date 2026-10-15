"""Wall-clock scheduler: durable due rows catch up on startup and during silence."""
import logging
import os
import time

import psycopg2
from prometheus_client import CollectorRegistry, Counter, Gauge, start_http_server

from .profiles import ProfileStore
from .segments import SEGMENTS, register_rules


def main():
    logging.basicConfig(level=logging.INFO)
    store = ProfileStore()
    settings = store.segment_settings
    connection = store.connect()
    try:
        with connection, connection.cursor() as cursor:
            register_rules(cursor, settings)
    finally:
        connection.close()
    registry = CollectorRegistry()
    evaluations = Counter('journeypulse_segment_evaluations_total', 'Customers evaluated.', registry=registry)
    failures = Counter('journeypulse_segment_evaluation_failures_total', 'Failed scheduler batches.', registry=registry)
    health = Gauge('journeypulse_segment_collection_healthy', 'Last scheduler cycle succeeded.', registry=registry)
    success = Gauge('journeypulse_segment_last_success_timestamp_seconds', 'Last successful scheduler cycle.', registry=registry)
    overdue = Gauge('journeypulse_segment_oldest_overdue_seconds', 'Oldest persisted evaluation deadline delay.', registry=registry)
    memberships = Gauge('journeypulse_segment_members', 'Persisted behavior members; independent of consent.',
                        ['segment'], registry=registry)
    start_http_server(int(os.getenv('JOURNEYPULSE_SEGMENT_METRICS_PORT', '8022')), registry=registry)
    while True:
        try:
            count = store.evaluate_due()
            evaluations.inc(count)
            counts, delay = store.segment_statistics()
            for name in SEGMENTS:
                memberships.labels(name).set(counts.get(name, 0))
            overdue.set(delay)
            health.set(1)
            success.set(time.time())
        except psycopg2.Error:
            health.set(0)
            failures.inc()
            logging.error('JourneyPulse segment database evaluation unavailable')
            count = 0
        # Drain full batches promptly during catch-up; avoid monopolizing the database.
        time.sleep(0.05 if count == settings.batch_size else settings.poll_seconds)


if __name__ == '__main__':
    main()
