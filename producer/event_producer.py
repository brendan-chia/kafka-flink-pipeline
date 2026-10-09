"""
Grab Event Producer
-------------------
Simulates a stream of user activity events being sent to Kafka.
Normal traffic is valid; the fault profile injects invalid records.
"""

import json
import random
import time
import logging
from uuid import uuid4
import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from event_contract import SCHEMA_VERSION

from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable
from faker import Faker

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [PRODUCER] %(message)s'
)
logger = logging.getLogger(__name__)

fake = Faker()

# ── Config ────────────────────────────────────────────────────────────────────
KAFKA_BOOTSTRAP = os.getenv('KAFKA_BOOTSTRAP', 'localhost:29092')
TOPIC = os.getenv('SOURCE_TOPIC', 'user-events')
EVENTS_PER_SECOND = float(os.getenv('EVENTS_PER_SECOND', '2'))
PRODUCER_PROFILE = os.getenv('PRODUCER_PROFILE', 'normal')
INVALID_EVENT_RATE = float(os.getenv('INVALID_EVENT_RATE', '0.20' if PRODUCER_PROFILE == 'fault' else '0'))
DELAYED_EVENT_RATE = float(os.getenv('DELAYED_EVENT_RATE', '0'))
MAX_EVENT_DELAY_SECONDS = int(os.getenv('MAX_EVENT_DELAY_SECONDS', '20'))
DUPLICATE_EVENT_RATE = float(os.getenv('DUPLICATE_EVENT_RATE', '0'))
if os.getenv('EVENT_RANDOM_SEED'):
    random.seed(os.environ['EVENT_RANDOM_SEED'])
    Faker.seed(os.environ['EVENT_RANDOM_SEED'])

# Valid event types that the Flink pipeline accepts
VALID_EVENT_TYPES = ['food_order', 'ride_request', 'payment', 'grocery_order']

# Invalid types injected to test DLQ
INVALID_EVENT_TYPES = ['HACK_ATTEMPT', 'unknown_event', 'null_type', '']


def connect_kafka(retries: int = 10, delay: int = 3) -> KafkaProducer:
    """Retry connecting to Kafka — it takes a few seconds to start."""
    for attempt in range(retries):
        try:
            producer = KafkaProducer(
                bootstrap_servers=[KAFKA_BOOTSTRAP],
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                acks='all',          # Wait for broker acknowledgement
                retries=3,
                max_in_flight_requests_per_connection=1,
            )
            logger.info("Connected to Kafka successfully.")
            return producer
        except NoBrokersAvailable:
            logger.warning(f"Kafka not ready yet. Retrying in {delay}s... ({attempt + 1}/{retries})")
            time.sleep(delay)
    raise RuntimeError("Could not connect to Kafka after multiple retries.")


def generate_valid_event() -> dict:
    """Generates a realistic, valid Grab user activity event."""
    timestamp = time.time_ns() // 1_000_000
    if random.random() < DELAYED_EVENT_RATE:
        timestamp -= random.randint(1, max(1, MAX_EVENT_DELAY_SECONDS * 1000))
    return {
        'schema_version': SCHEMA_VERSION,
        'event_id': str(uuid4()),
        'currency': 'MYR',
        'user_id': f'user_{fake.numerify("####")}',
        'event_type': random.choice(VALID_EVENT_TYPES),
        'timestamp': timestamp,
        'amount': round(random.uniform(1.0, 150.0), 2),
    }


def generate_invalid_event() -> dict:
    """Generates an intentionally malformed event for DLQ testing."""
    invalid_choice = random.choice(['null_user', 'bad_type', 'negative_amount', 'multiple_errors'])

    event = generate_valid_event()
    if invalid_choice in ('null_user', 'multiple_errors'):
        event['user_id'] = None
    if invalid_choice in ('bad_type', 'multiple_errors'):
        event['event_type'] = random.choice(INVALID_EVENT_TYPES)
    if invalid_choice in ('negative_amount', 'multiple_errors'):
        event['amount'] = round(random.uniform(-50.0, -0.01), 2)
    return event


def main():
    if PRODUCER_PROFILE not in ('normal', 'fault'):
        raise ValueError('PRODUCER_PROFILE must be normal or fault.')
    if EVENTS_PER_SECOND <= 0 or MAX_EVENT_DELAY_SECONDS < 1 or not all(
            0 <= rate <= 1 for rate in (INVALID_EVENT_RATE, DELAYED_EVENT_RATE, DUPLICATE_EVENT_RATE)):
        raise ValueError('Rates must be between 0 and 1; throughput and maximum delay must be positive.')
    producer = connect_kafka()
    sent_count = 0
    invalid_count = 0
    previous_valid = None

    logger.info(f"Sending events to topic '{TOPIC}' at ~{EVENTS_PER_SECOND} events/sec.")
    logger.info("Press Ctrl+C to stop.\n")

    try:
        while True:
            # Fault injection is opt-in via profile or explicit rate.
            is_invalid = random.random() < INVALID_EVENT_RATE

            event = generate_invalid_event() if is_invalid else generate_valid_event()
            if not is_invalid:
                if previous_valid is not None and random.random() < DUPLICATE_EVENT_RATE:
                    event = previous_valid.copy()
                else:
                    previous_valid = event.copy()
            # Count only broker-acknowledged records. Retries keep the same event ID.
            key = (event.get('user_id') or event['event_id']).encode('utf-8')
            producer.send(TOPIC, key=key, value=event).get(timeout=30)
            sent_count += 1

            if is_invalid:
                invalid_count += 1
                logger.info(f"[INVALID] #{sent_count} → {event}")
            else:
                logger.info(f"[VALID]   #{sent_count} → {event}")

            if sent_count % 20 == 0:
                logger.info(f"── Summary: {sent_count} total sent, {invalid_count} invalid ({invalid_count/sent_count*100:.0f}%) ──")

            time.sleep(1.0 / EVENTS_PER_SECOND)

    except KeyboardInterrupt:
        logger.info(f"\nStopped. Total: {sent_count} events sent, {invalid_count} invalid.")
    finally:
        producer.flush()
        producer.close()


if __name__ == '__main__':
    main()
