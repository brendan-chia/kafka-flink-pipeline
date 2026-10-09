"""Reconcile writes, abruptly stop the TaskManager, and prove checkpoint replay.

Only for the opt-in local runtime stack. No topic/table deletion. The command
requires --inject-taskmanager-failure and a checkpoint interval >= 10 minutes.
"""
import argparse
from datetime import datetime, timezone
import json
from pathlib import Path
import subprocess
import time
from urllib.request import Request, urlopen

import recovery_probe

ROOT = Path(__file__).resolve().parents[1]
COMPOSE = ['docker', 'compose', '-f', str(ROOT / 'docker-compose.yml'),
           '-f', str(ROOT / 'docker-compose.runtime.yml')]


def request(base_url, path, body=None):
    data = None if body is None else json.dumps(body).encode()
    req = Request(base_url.rstrip('/') + path, data=data,
                  headers={'Content-Type': 'application/json'})
    with urlopen(req, timeout=10) as response:
        return json.load(response)


def wait_for(function, timeout):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        value = function()
        if value:
            return value
        time.sleep(1)
    raise TimeoutError('Flink did not reach the required recovery state in time.')


def select_job(overview):
    active = [job for job in overview['jobs'] if job['state'] not in ('FINISHED', 'FAILED', 'CANCELED')]
    if len(active) != 1 or active[0]['state'] != 'RUNNING':
        raise RuntimeError('Require exactly one active, RUNNING job in this local cluster.')
    return active[0]['jid']


def require_pre_checkpoint(stats, checkpoint_id):
    latest = stats.get('latest', {}).get('completed') or {}
    if latest.get('id') != checkpoint_id or stats['counts']['in_progress']:
        raise RuntimeError('A newer checkpoint started/completed; refuse an inconclusive drill. '
                           'Retry with a fresh output and a longer checkpoint interval.')


def run(args, report):
    def ready_job():
        overview = request(args.flink_url, '/jobs/overview')
        active = [job for job in overview['jobs'] if job['state'] not in ('FINISHED', 'FAILED', 'CANCELED')]
        if len(active) > 1:
            raise RuntimeError('Require exactly one active job in this local cluster.')
        if active and active[0]['state'] == 'RUNNING':
            return select_job(overview)
        return None

    job = wait_for(ready_job, args.timeout)
    prefix = f'/jobs/{job}'
    config = request(args.flink_url, prefix + '/checkpoints/config')
    if config['interval'] < 600000:
        raise RuntimeError('Set FLINK_CHECKPOINT_INTERVAL="10 min" before submitting this drill job.')
    report['job_id'] = job
    trigger = request(args.flink_url, prefix + '/checkpoints', {})['request-id']

    def completed_checkpoint():
        result = request(args.flink_url, prefix + '/checkpoints/' + trigger)
        if result['status']['id'] != 'COMPLETED':
            return None
        operation = result['operation']
        if 'failure-cause' in operation:
            raise RuntimeError(str(operation['failure-cause']))
        return operation['checkpointId']

    checkpoint_id = int(wait_for(completed_checkpoint, args.timeout))
    report['baseline_checkpoint_id'] = checkpoint_id
    manifest = args.output.with_suffix('.manifest.json')
    recovery_probe.publish(manifest)
    report['manifest'] = str(manifest)
    report['before_failure'] = recovery_probe.verify(manifest, args.timeout)
    require_pre_checkpoint(request(args.flink_url, prefix + '/checkpoints'), checkpoint_id)
    started = time.monotonic()
    # Killing the service is the explicit fault-injection action. Always bring
    # it back, including when docker kill fails after delivery to the daemon.
    try:
        subprocess.run(COMPOSE + ['kill', '-s', 'SIGKILL', 'taskmanager'], check=True)
    finally:
        subprocess.run(COMPOSE + ['start', 'taskmanager'], check=True)

    def restored():
        state = request(args.flink_url, prefix)['state']
        stats = request(args.flink_url, prefix + '/checkpoints')
        snapshot = stats.get('latest', {}).get('restored') or {}
        return state == 'RUNNING' and snapshot.get('id') == checkpoint_id

    wait_for(restored, args.timeout)
    # Seeing the same audit IDs is insufficient: at least two DLQ deliveries
    # per source record also prove this fixture was read again from Kafka.
    report['after_failure'] = recovery_probe.verify(manifest, args.timeout, min_dlq_deliveries=2)
    report['recovery_seconds'] = round(time.monotonic() - started, 3)
    report['status'] = 'passed'


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--inject-taskmanager-failure', action='store_true', required=True)
    parser.add_argument('--flink-url', default='http://localhost:8081')
    parser.add_argument('--timeout', type=float, default=180)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    if args.timeout <= 0 or args.output.exists() or args.output.with_suffix('.manifest.json').exists():
        parser.error('Timeout must be positive and output/manifest paths must be new.')
    args.output.parent.mkdir(parents=True, exist_ok=True)
    report = {'status': 'failed', 'started_at': datetime.now(timezone.utc).isoformat(),
              'scenario': 'taskmanager_loss_after_writes_before_checkpoint'}
    try:
        run(args, report)
    except Exception as exc:
        report['error'] = f'{type(exc).__name__}: {exc}'
        raise
    finally:
        report['finished_at'] = datetime.now(timezone.utc).isoformat()
        args.output.write_text(json.dumps(report, indent=2) + '\n', encoding='utf-8')


if __name__ == '__main__':
    main()
