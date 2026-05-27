"""
Grab Event Producer
-------------------
Simulates a stream of user activity events being sent to Kafka.
Intentionally injects ~20% invalid events to demonstrate DLQ routing.
"""

import json
import random
import time
import logging
from datetime import datetime

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
KAFKA_BOOTSTRAP = 'localhost:9092'
TOPIC = 'user-events'
EVENTS_PER_SECOND = 2   # Adjust as needed

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
            )
            logger.info("Connected to Kafka successfully.")
            return producer
        except NoBrokersAvailable:
            logger.warning(f"Kafka not ready yet. Retrying in {delay}s... ({attempt + 1}/{retries})")
            time.sleep(delay)
    raise RuntimeError("Could not connect to Kafka after multiple retries.")


def generate_valid_event() -> dict:
    """Generates a realistic, valid Grab user activity event."""
    return {
        'user_id': f'user_{fake.numerify("####")}',
        'event_type': random.choice(VALID_EVENT_TYPES),
        'timestamp': int(datetime.utcnow().timestamp() * 1000),  # milliseconds
        'amount': round(random.uniform(1.0, 150.0), 2),
    }


def generate_invalid_event() -> dict:
    """Generates an intentionally malformed event for DLQ testing."""
    invalid_choice = random.choice(['null_user', 'bad_type', 'negative_amount', 'multiple_errors'])

    if invalid_choice == 'null_user':
        return {
            'user_id': None,
            'event_type': random.choice(VALID_EVENT_TYPES),
            'timestamp': int(datetime.utcnow().timestamp() * 1000),
            'amount': round(random.uniform(1.0, 50.0), 2),
        }
    elif invalid_choice == 'bad_type':
        return {
            'user_id': f'user_{fake.numerify("####")}',
            'event_type': random.choice(INVALID_EVENT_TYPES),
            'timestamp': int(datetime.utcnow().timestamp() * 1000),
            'amount': round(random.uniform(1.0, 50.0), 2),
        }
    elif invalid_choice == 'negative_amount':
        return {
            'user_id': f'user_{fake.numerify("####")}',
            'event_type': random.choice(VALID_EVENT_TYPES),
            'timestamp': int(datetime.utcnow().timestamp() * 1000),
            'amount': round(random.uniform(-50.0, -0.01), 2),
        }
    else:  # multiple_errors
        return {
            'user_id': None,
            'event_type': random.choice(INVALID_EVENT_TYPES),
            'timestamp': int(datetime.utcnow().timestamp() * 1000),
            'amount': round(random.uniform(-10.0, -0.01), 2),
        }


def main():
    producer = connect_kafka()
    sent_count = 0
    invalid_count = 0

    logger.info(f"Sending events to topic '{TOPIC}' at ~{EVENTS_PER_SECOND} events/sec.")
    logger.info("Press Ctrl+C to stop.\n")

    try:
        while True:
            # Inject ~20% invalid events
            is_invalid = random.random() < 0.20

            event = generate_invalid_event() if is_invalid else generate_valid_event()
            producer.send(TOPIC, value=event)
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