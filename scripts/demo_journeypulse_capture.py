"""Emit the five Phase 1 events plus an invalid cart through the live gateway."""
import argparse
from copy import deepcopy
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import sys
from urllib.error import HTTPError
from urllib.request import Request, urlopen
from uuid import uuid4

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from journeypulse.auth import DEFAULT_SOURCE_KEY
from journeypulse.contracts import ROOT


def request(url, body, headers):
    req = Request(url, data=json.dumps(body).encode(), headers={
        'Content-Type': 'application/json', **headers}, method='POST')
    try:
        response = urlopen(req, timeout=15)
    except HTTPError as error:
        response = error
    with response:
        return response.status, json.load(response)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--url', default='http://127.0.0.1:8020')
    parser.add_argument('--source', default='demo-app')
    args = parser.parse_args()
    suffix = uuid4().hex[:12]
    identity = {'source_id': args.source, 'user_id': 'demo_' + suffix,
                'session_id': 'session_' + suffix}
    status, session = request(args.url + '/v1/sessions', identity,
        {'X-Source-Key': os.getenv('JOURNEYPULSE_SOURCE_KEY', DEFAULT_SOURCE_KEY)})
    if status != 200:
        raise SystemExit('Session issuance failed: HTTP ' + str(status))
    events = json.loads((ROOT / 'contracts/journeypulse/examples/valid-journey.json').read_text())
    for event in events:
        event.update(event_id='evt_' + uuid4().hex, user_id=identity['user_id'],
                     session_id=identity['session_id'],
                     occurred_at=datetime.now(timezone.utc).isoformat().replace('+00:00', 'Z'))
    invalid = deepcopy(events[2])
    invalid['event_id'] = 'evt_' + uuid4().hex
    invalid['properties']['quantity'] = 0
    del invalid['properties']['item_id']
    status, result = request(args.url + '/v1/events', {'events': events + [invalid]},
        {'Authorization': 'Bearer ' + session['access_token']})
    print(json.dumps({'http_status': status, 'user_id': identity['user_id'],
                      'result': result}, indent=2))
    if result.get('counts') != {'accepted': 5, 'rejected': 1, 'retryable': 0}:
        raise SystemExit('Capture demo did not produce the expected acknowledged outcomes.')
    if not result['results'][-1].get('quarantine_persisted'):
        raise SystemExit('Invalid cart quarantine was not acknowledged.')


if __name__ == '__main__':
    main()
