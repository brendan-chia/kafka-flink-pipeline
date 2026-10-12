"""HTTP -> Kafka -> actual PyFlink -> customer API, plus optional fresh-job replay."""
import argparse
from copy import deepcopy
from datetime import datetime, timedelta, timezone
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
from journeypulse.contracts import ROOT, EVENT_NAMES


def wait_for(action, predicate, timeout=180):
    deadline = time.monotonic()+timeout
    last = None
    while time.monotonic() < deadline:
        try:
            last = action()
            if predicate(last):
                return last
        except httpx.HTTPError:
            pass
        time.sleep(1)
    raise AssertionError(f'Timed out waiting for profiles: {last}')


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--restart', action='store_true', help='Cancel and resubmit the dedicated Compose job from earliest offsets')
    args = parser.parse_args()
    gateway = os.getenv('JOURNEYPULSE_GATEWAY_URL', 'http://127.0.0.1:8020')
    serving = os.getenv('JOURNEYPULSE_CUSTOMERS_URL', 'http://127.0.0.1:8021')
    flink = os.getenv('JOURNEYPULSE_FLINK_URL', 'http://127.0.0.1:8082')
    user, session = 'profilecheck-'+uuid4().hex, uuid4().hex
    source = os.getenv('JOURNEYPULSE_SOURCE_ID', 'demo-app')
    with httpx.Client(timeout=10) as client:
        for url in (gateway, serving):
            wait_for(lambda: client.get(url+'/v1/health/ready'), lambda r: r.status_code == 200)
        credentials = dict(source_id=source, user_id=user, session_id=session)
        headers = {'X-Source-Key': os.getenv('JOURNEYPULSE_SOURCE_KEY', DEFAULT_SOURCE_KEY)}
        tokens = []
        for url in (gateway, serving):
            result = client.post(url+'/v1/sessions', headers=headers, json=credentials)
            result.raise_for_status()
            tokens.append({'Authorization': 'Bearer '+result.json()['access_token']})
        tracking, customer = tokens
        assert client.get(serving+'/v1/customers/'+user, headers=customer).status_code == 404
        events = json.loads((ROOT/'contracts/journeypulse/examples/valid-journey.json').read_text())
        now = datetime.now(timezone.utc).replace(microsecond=0)
        for event in events:
            event.update(user_id=user, session_id=session, event_id=uuid4().hex, occurred_at=now.isoformat())
        def send(batch):
            result = client.post(gateway+'/v1/events', headers=tracking, json={'events': batch})
            assert result.status_code == 200, result.text
            return result.json()
        start = time.monotonic()
        deliveries = send(events)
        send(events)  # Different offsets, stable event IDs.
        late = deepcopy(events[1])
        late.update(event_id=uuid4().hex, occurred_at=(now-timedelta(days=1)).isoformat())
        repeated_order = deepcopy(events[4])
        repeated_order.update(event_id=uuid4().hex)
        send([late, repeated_order])
        def read():
            result = client.get(serving+'/v1/customers/'+user, headers=customer)
            return result.json() if result.status_code == 200 else {}
        expected = dict.fromkeys(EVENT_NAMES, 1)
        expected.update(restaurant_view=2, order_completed=2)
        row = wait_for(read, lambda r: r.get('event_counts') == expected)
        freshness = time.monotonic()-start
        assert row['completed_orders'] == 1, row
        assert datetime.fromisoformat(row['latest_activity_at'].replace('Z', '+00:00')) == now, row
        assert not row['marketing_consent'] and row['onboarded_at'] is None
        response = client.post(serving+'/v1/customers', headers=customer,
                               json={'preferred_language': 'ms', 'marketing_consent': True})
        response.raise_for_status()
        extra = deepcopy(events[0]); extra['event_id'] = uuid4().hex
        send([extra]); expected['session_started'] = 2
        row = wait_for(read, lambda r: r.get('event_counts') == expected)
        assert row['marketing_consent'] and row['preferred_language'] == 'ms'
        assert client.get(serving+'/v1/customers/other', headers=customer).status_code == 403
        assert client.patch(serving+'/v1/customers/other', headers=customer,
                            json={'marketing_consent': False}).status_code == 403
        if args.restart:
            jobs = client.get(flink+'/jobs/overview').json()['jobs']
            job = next(j for j in jobs if j['name'] == 'JourneyPulse profiles v1' and j['state'] == 'RUNNING')
            # Wait for a completed source checkpoint before cancelling.
            wait_for(lambda: client.get(flink+'/jobs/'+job['jid']+'/checkpoints').json(),
                     lambda r: r.get('counts', {}).get('completed', 0) > 0)
            before = deepcopy(row)
            cancel = client.patch(flink+'/jobs/'+job['jid'], params={'mode': 'cancel'})
            cancel.raise_for_status()
            wait_for(lambda: client.get(flink+'/jobs/'+job['jid']).json(), lambda r: r.get('state') == 'CANCELED')
            subprocess.run(['docker', 'compose', '-f', 'docker-compose.yml', '-f', 'docker-compose.journeypulse.yml',
                            'up', '-d', '--no-deps', '--force-recreate', 'journeypulse-processor'], cwd=ROOT, check=True)
            new_job = wait_for(lambda: client.get(flink+'/jobs/overview').json(),
                lambda r: any(j['jid'] != job['jid'] and j['name'] == job['name'] and j['state'] == 'RUNNING' for j in r.get('jobs', [])))
            new_id = next(j['jid'] for j in new_job['jobs'] if j['jid'] != job['jid'] and j['state'] == 'RUNNING')
            # Prove replay has reached the sink, rather than just observing an idle API.
            metrics_url = flink+'/jobs/'+new_id+'/vertices/'
            details = client.get(flink+'/jobs/'+new_id).json()
            vertex = next(v for v in details['vertices'] if 'journeypulse-profile-writes' in v['name'])
            base = metrics_url+vertex['id']+'/subtasks/0/metrics'
            metrics = wait_for(lambda: client.get(base).json(), lambda r: any(m['id'].endswith('replay') for m in r))
            metric_id = next(m['id'] for m in metrics if m['id'].endswith('replay'))
            wait_for(lambda: client.get(base, params={'get': metric_id}).json(),
                     lambda r: bool(r) and int(r[0]['value']) >= len(events)+3)
            row = read()
            for field in ('event_counts', 'completed_orders', 'revision', 'processed_at', 'latest_activity_at'):
                if field == 'processed_at':
                    assert row['freshness'][field] == before['freshness'][field]
                else:
                    assert row[field] == before[field], (field, before, row)
        print(json.dumps(dict(status='ready', user_id=user, event_to_profile_seconds=freshness,
                              restart_replay_verified=args.restart, first_deliveries=deliveries['counts'])))


if __name__ == '__main__':
    main()
