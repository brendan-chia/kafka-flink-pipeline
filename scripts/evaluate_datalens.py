"""Evaluate fixed investigations; no credentials or external services needed by default."""
import argparse
import json
from pathlib import Path
import sys
from psycopg2 import OperationalError
from psycopg2.errors import InsufficientPrivilege
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from datalens.benchmark import run_benchmark


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--live-model', action='store_true', help='Opt in to six hosted requests and model cost')
    parser.add_argument('--postgres', action='store_true', help='Use a newly created disposable database; requires CREATEDB')
    parser.add_argument('--output', type=Path, default=Path('.flink-state/datalens-evaluation.json'))
    args = parser.parse_args()
    try:
        if args.postgres:
            from datalens.evaluation_db import benchmark_database
            with benchmark_database() as factory:
                result = run_benchmark(args.live_model, factory)
        else:
            result = run_benchmark(args.live_model)
        result['backend'] = 'postgres' if args.postgres else 'offline_fixture'
    except Exception as exc:
        # Never serialize connection strings, provider errors, environment values or keys.
        result = dict(schema_version=1,
                      status='blocked' if isinstance(exc, (OperationalError, InsufficientPrivilege, OSError)) else 'failed', cases=[],
                      error_type=type(exc).__name__,
                      reason='Evaluation failed; check service availability, database privileges and price configuration locally')
    result['backend'] = 'postgres' if args.postgres else 'offline_fixture'
    result.setdefault('mode', 'live_model' if args.live_model else 'deterministic')
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(result, indent=2) + '\n', encoding='utf-8')
    print(json.dumps({k:v for k,v in result.items() if k != 'cases'}, indent=2))
    return 0 if result['status'] == 'passed' else 2 if result['status'] == 'blocked' else 1


if __name__ == '__main__':
    sys.exit(main())
