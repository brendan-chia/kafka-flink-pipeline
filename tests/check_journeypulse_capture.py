"""Live HTTP/SDK/Kafka integration probe; publishes uniquely identified test journeys."""
import argparse
import base64
from collections import Counter
import json
import os
from pathlib import Path
import subprocess
import sys
import time
from urllib.error import HTTPError
from urllib.request import Request, urlopen

from kafka import KafkaConsumer, TopicPartition
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from journeypulse.auth import DEFAULT_SOURCE_KEY
from journeypulse.config import Settings, TOPICS, EVENT_TOPIC, QUARANTINE_TOPIC
from journeypulse.contracts import validation_errors

ROOT = Path(__file__).resolve().parents[1]


def call(url, raw, headers):
    request = Request(url, data=raw, headers={'Content-Type': 'application/json', **headers})
    try:
        response = urlopen(request, timeout=15)
    except HTTPError as error:
        response = error
    with response:
        return response.status, json.load(response)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--url', default='http://127.0.0.1:8020')
    args = parser.parse_args()
    deadline = time.monotonic() + 60
    while True:
        try:
            with urlopen(args.url + '/v1/health/ready', timeout=3) as response:
                assert response.status == 200
            break
        except (OSError, AssertionError):
            if time.monotonic() >= deadline:
                raise SystemExit('Gateway/Kafka did not become ready')
            time.sleep(1)

    consumer = KafkaConsumer(bootstrap_servers=Settings.from_environment().kafka_bootstrap,
                             enable_auto_commit=False, api_version=(2, 6, 0))
    try:
        partitions = [TopicPartition(topic, number) for topic in TOPICS
                      for number in (consumer.partitions_for_topic(topic) or ())]
        if len(partitions) != 6:
            raise SystemExit('Expected both three-partition JourneyPulse topics')
        consumer.assign(partitions)
        consumer.seek_to_end(*partitions)
        # Materialize positions before publication; do not accidentally skip fresh deliveries.
        for partition in partitions:
            consumer.position(partition)
        environment = dict(os.environ, JOURNEYPULSE_GATEWAY_URL=args.url)
        completed = subprocess.run(['node', str(ROOT / 'scripts/demo_journeypulse_sdk.mjs')],
                                   env=environment, capture_output=True, text=True, timeout=90)
        if completed.returncode:
            raise SystemExit('SDK capture probe failed: ' + completed.stderr)
        report = json.loads(completed.stdout)
        identity = report['identity']
        status, session = call(args.url + '/v1/sessions', json.dumps(identity).encode(),
            {'X-Source-Key': os.getenv('JOURNEYPULSE_SOURCE_KEY', DEFAULT_SOURCE_KEY)})
        assert status == 200
        headers = {'Authorization': 'Bearer ' + session['access_token']}
        duplicate = report['outcomes'][0]['event']
        status, response = call(args.url + '/v1/events',
                               json.dumps({'events': [duplicate]}).encode(), headers)
        assert status == 200 and response['counts']['accepted'] == 1
        raw_invalid = b'{broken-json'
        status, response = call(args.url + '/v1/events', raw_invalid, headers)
        assert status == 400 and response['detail']['quarantine_persisted']
        mismatch = dict(duplicate, user_id='another_user')
        status, response = call(args.url + '/v1/events',
                               json.dumps({'events': [mismatch]}).encode(), headers)
        assert status == 207 and response['results'][0]['status'] == 'rejected'

        source, quarantine = [], []
        deadline = time.monotonic() + 30
        while time.monotonic() < deadline and (len(source) < 6 or len(quarantine) < 3):
            for records in consumer.poll(timeout_ms=1000).values():
                for message in records:
                    value = json.loads(message.value)
                    user = value['event']['user_id'] if message.topic == EVENT_TOPIC else value['user_id']
                    if user != identity['user_id']:
                        continue
                    assert message.key == identity['user_id'].encode()
                    assert value['source_id'] == identity['source_id']
                    assert value['received_at'].endswith('Z')
                    (source if message.topic == EVENT_TOPIC else quarantine).append((message, value))
        assert len(source) == 6 and len(quarantine) == 3, (len(source), len(quarantine))
        assert len({message.partition for message, _ in source}) == 1
        assert all(not validation_errors(value['event']) for _, value in source)
        counts = Counter(value['event']['event_id'] for _, value in source)
        assert counts[duplicate['event_id']] == 2  # Phase 2 acknowledges deliveries, not deduplicated profiles.
        assert any(base64.b64decode(value['raw_request_base64']) == raw_invalid for _, value in quarantine)
        assert {value['reason'] for _, value in quarantine} == {'schema', 'malformed_json', 'identity'}
        print('PASS: SDK -> authenticated gateway -> six acknowledged Kafka deliveries; '
              'user partition key, duplicate delivery, three lossless quarantine records and identity binding.')
    finally:
        consumer.close()


if __name__ == '__main__':
    main()
