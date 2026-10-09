"""DataLens evidence CLI. All commands except migrate are read-only."""
import argparse
import json
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from datalens.evidence import compare_revenue, connect, connection_params, json_value
from psycopg2.extras import RealDictCursor


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest='command', required=True)
    sub.add_parser('migrate', help='Apply repeatable migration 004 to the existing database')
    sub.add_parser('catalogue', help='Show metric definitions, datasets and dependencies')
    quality = sub.add_parser('quality', help='Read recent persisted quality evidence')
    quality.add_argument('--limit', type=int, default=20)
    compare = sub.add_parser('compare-revenue', help='Compare completed windows to current unique payment audit')
    compare.add_argument('--from-utc', required=True)
    compare.add_argument('--until-utc', required=True)
    compare.add_argument('--currency', default='MYR')
    compare.add_argument('--window-seconds', type=int, default=300)
    args = parser.parse_args()
    params = connection_params()
    if args.command == 'compare-revenue':
        result = compare_revenue(params, args.from_utc, args.until_utc, args.currency, args.window_seconds)
    else:
        if args.command == 'quality' and not 1 <= args.limit <= 1000:
            parser.error('--limit must be in 1..1000')
        conn = connect(params, readonly=args.command != 'migrate')
        try:
            with conn:
                with conn.cursor(cursor_factory=RealDictCursor) as cur:
                    if args.command == 'migrate':
                        sql = (Path(__file__).resolve().parents[1] / 'sql/migrations/004_datalens_evidence.sql').read_text()
                        cur.execute(sql.replace('BEGIN;', '').replace('COMMIT;', ''))
                        result = {'migration': '004_datalens_evidence', 'status': 'applied'}
                    elif args.command == 'catalogue':
                        result = {}
                        for table in ('datasets', 'metrics', 'dependencies'):
                            cur.execute('SELECT * FROM datalens.' + table + ' ORDER BY 1')
                            result[table] = [dict(row) for row in cur.fetchall()]
                    else:
                        cur.execute('SELECT * FROM datalens.quality_results ORDER BY observed_at DESC LIMIT %s', (args.limit,))
                        result = [dict(row) for row in cur.fetchall()]
        finally:
            conn.close()
    print(json.dumps(result, default=json_value, indent=2, allow_nan=False))


if __name__ == '__main__':
    main()
