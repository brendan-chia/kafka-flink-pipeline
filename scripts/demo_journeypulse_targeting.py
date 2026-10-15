"""Executable gateway -> Kafka -> Flink -> silent abandonment -> targeting scenario.

--restart-worker exercises downtime on the dedicated JourneyPulse Compose worker.
The default waits ten minutes; configure short thresholds consistently for CI.
"""
import argparse
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import subprocess
import sys
import time
from uuid import uuid4

import httpx

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from journeypulse.auth import DEFAULT_SOURCE_KEY
from journeypulse.contracts import ROOT
from journeypulse.segments import SegmentSettings


def wait_for(action, predicate, timeout):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        result = action()
        if predicate(result):
            return result
        time.sleep(1)
    raise AssertionError('Timed out waiting for JourneyPulse targeting transition')


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--restart-worker', action='store_true',
        help='Stop only the dedicated segment worker across the cart deadline, then restart it')
    args = parser.parse_args()
    settings = SegmentSettings.from_environment()
    gateway = os.getenv('JOURNEYPULSE_GATEWAY_URL', 'http://127.0.0.1:8020')
    serving = os.getenv('JOURNEYPULSE_CUSTOMERS_URL', 'http://127.0.0.1:8021')
    source = os.getenv('JOURNEYPULSE_SOURCE_ID', 'demo-app')
    user, session, cart = 'targetcheck-'+uuid4().hex, uuid4().hex, uuid4().hex
    compose = ['docker', 'compose', '-f', 'docker-compose.yml', '-f', 'docker-compose.journeypulse.yml']
    with httpx.Client(timeout=10) as client:
        identity = dict(source_id=source, user_id=user, session_id=session)
        key = {'X-Source-Key': os.getenv('JOURNEYPULSE_SOURCE_KEY', DEFAULT_SOURCE_KEY)}
        headers = []
        for url in (gateway, serving):
            wait_for(lambda: client.get(url+'/v1/health/ready'), lambda r: r.status_code == 200, 180)
            result = client.post(url+'/v1/sessions', headers=key, json=identity)
            result.raise_for_status()
            headers.append({'Authorization': 'Bearer '+result.json()['access_token']})
        tracking, customer = headers
        result = client.post(serving+'/v1/customers', headers=customer,
            json={'preferred_language': 'en', 'marketing_consent': True})
        result.raise_for_status()
        flink = os.getenv('JOURNEYPULSE_FLINK_URL', 'http://127.0.0.1:8082')
        wait_for(lambda: client.get(flink+'/jobs/overview').json(),
                 lambda r: any(j['name'] == 'JourneyPulse profiles v1' and j['state'] == 'RUNNING'
                               for j in r.get('jobs', [])), 180)
        now = datetime.now(timezone.utc)
        add = dict(schema_version=1, event_id=uuid4().hex, user_id=user, session_id=session,
            occurred_at=now.isoformat(), event_name='add_to_cart',
            properties=dict(cart_id=cart,restaurant_id='r1',item_id='i1',quantity=1))

        def send(event):
            result = client.post(gateway+'/v1/events', headers=tracking, json={'events': [event]})
            result.raise_for_status()
            assert result.json()['counts']['accepted'] == 1, result.text

        def read():
            result = client.get(serving+'/v1/customers/'+user+'/targeting', headers=customer)
            result.raise_for_status()
            return result.json()

        # Warm the source/sink before a short deadline, then start the timed cart.
        send(dict(add, event_id=uuid4().hex, event_name='session_started', properties={'platform': 'web'}))
        wait_for(read, lambda r: r['freshness']['status'] == 'current', 180)
        now = datetime.now(timezone.utc)
        add['occurred_at'] = now.isoformat()
        send(add)
        def cart_applied():
            result = client.get(serving+'/v1/customers/'+user, headers=customer)
            result.raise_for_status()
            return result.json()['event_counts']['add_to_cart'] == 1
        wait_for(cart_applied, bool, 180)
        initial = wait_for(read, lambda r: r['freshness']['status'] == 'current', 180)
        assert not initial['eligible'], initial
        started = time.monotonic()
        if args.restart_worker:
            subprocess.run(compose+['stop', 'journeypulse-segments'], cwd=ROOT, check=True)
            try:
                # No additional clickstream event triggers the transition.
                deadline = now.timestamp()+settings.abandon_seconds+settings.poll_seconds
                while time.time() < deadline:
                    time.sleep(min(1, deadline-time.time()))
            finally:
                subprocess.run(compose+['up', '-d', '--no-deps', 'journeypulse-segments'], cwd=ROOT, check=True)
        abandoned = wait_for(read, lambda r: r['segments']['abandoned_cart']['eligible'],
                             settings.abandon_seconds+180)
        assert abandoned['rule_version'] == settings.version, abandoned
        send(add)  # Another Kafka delivery cannot reset the deadline.
        assert read()['segments']['abandoned_cart']['eligible']
        result = client.patch(serving+'/v1/customers/'+user, headers=customer,
                              json={'marketing_consent': False})
        result.raise_for_status()
        assert not read()['eligible']
        result = client.patch(serving+'/v1/customers/'+user, headers=customer,
                              json={'marketing_consent': True})
        result.raise_for_status()
        # Delayed completion occurred before abandonment, but arrives afterwards.
        complete = dict(add, event_id=uuid4().hex, event_name='order_completed',
                        properties=dict(cart_id=cart, restaurant_id='r1', order_id=uuid4().hex))
        send(complete)
        corrected = wait_for(read, lambda r: r['segments']['abandoned_cart']['member'] is False, 180)
        assert not corrected['eligible'], corrected
        print(json.dumps(dict(status='ready', user_id=user, rule_version=settings.version,
            scheduler_restart_verified=args.restart_worker, elapsed_seconds=time.monotonic()-started,
            abandoned_eligible=abandoned['segments']['abandoned_cart']['eligible'],
            completion_eligible=corrected['segments']['abandoned_cart']['eligible'],
            freshness=corrected['freshness']['status'])))


if __name__ == '__main__':
    main()
