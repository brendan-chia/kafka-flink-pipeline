"""Bounded Kafka delivery; a queued record is not an acknowledged acceptance."""
from dataclasses import dataclass
import json
import threading
import time

from kafka import KafkaProducer
from kafka.admin import KafkaAdminClient

from .config import Settings, TOPICS, PARTITIONS


@dataclass(frozen=True)
class Record:
    topic: str
    key: str
    value: dict


class KafkaPublisher:
    def __init__(self, bootstrap=None, producer_factory=KafkaProducer, admin_factory=KafkaAdminClient):
        self.bootstrap = bootstrap or Settings.from_environment().kafka_bootstrap
        self.factory = producer_factory
        self.admin_factory = admin_factory
        self.producer = None
        self.lock = threading.Lock()

    def _producer(self):
        with self.lock:
            if self.producer is None:
                self.producer = self.factory(
                    bootstrap_servers=self.bootstrap, client_id='journeypulse-gateway',
                    # The local broker supports the 2.6 protocol. Avoid blocking API discovery
                    # when Kafka is down; the gateway stays alive and reports retryable delivery.
                    api_version=(2, 6, 0), acks='all', retries=3,
                    max_in_flight_requests_per_connection=1,
                    max_block_ms=200, request_timeout_ms=3000,
                    linger_ms=0, buffer_memory=8388608, max_request_size=2097152,
                    key_serializer=lambda value: value.encode('utf-8'),
                    value_serializer=lambda value: json.dumps(value, ensure_ascii=True,
                        allow_nan=False, separators=(',', ':')).encode('utf-8'))
            return self.producer

    def publish_many(self, records, timeout=5):
        deadline = time.monotonic() + timeout
        outcomes = [None] * len(records)
        futures = []
        try:
            producer = self._producer()
        except Exception:
            return outcomes
        for index, record in enumerate(records):
            if time.monotonic() >= deadline:
                break
            try:
                future = producer.send(record.topic, key=record.key, value=record.value)
                futures.append((index, future))
            except Exception:
                pass
        for index, future in futures:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                # Already completed futures may still have acknowledged outcomes.
                remaining = 0
            try:
                metadata = future.get(timeout=remaining)
                outcomes[index] = {'topic': metadata.topic,
                                   'partition': metadata.partition, 'offset': metadata.offset}
            except Exception:
                # Timeout is ambiguous: the record may still be delivered. Retain its ID on retry.
                pass
        return outcomes

    def ready(self):
        admin = None
        try:
            # Query the broker, rather than trusting cached producer metadata or
            # bootstrap_connected(), which can be false after normal broker discovery.
            admin = self.admin_factory(bootstrap_servers=self.bootstrap,
                client_id='journeypulse-readiness', api_version=(2, 6, 0),
                request_timeout_ms=1000, api_version_auto_timeout_ms=1000)
            topics = admin.describe_topics(list(TOPICS))
            return ({topic['topic'] for topic in topics} == set(TOPICS) and all(
                topic['error_code'] == 0 and len(topic['partitions']) == PARTITIONS
                and all(partition['leader'] >= 0 for partition in topic['partitions'])
                for topic in topics))
        except Exception:
            return False
        finally:
            if admin is not None:
                admin.close()

    def close(self):
        with self.lock:
            if self.producer is not None:
                self.producer.close(timeout=1)
                self.producer = None
