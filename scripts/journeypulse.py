"""Validate local event fixtures or bootstrap/check JourneyPulse infrastructure."""
import argparse
import json
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from journeypulse.contracts import ROOT, decode_json, validation_errors


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest='command', required=True)
    validate = commands.add_parser('validate', help='Validate a JSON event or array; no services needed')
    validate.add_argument('path', nargs='?', type=Path,
                          default=ROOT / 'contracts/journeypulse/examples/valid-journey.json')
    commands.add_parser('bootstrap', help='Create missing topics/schema and register immutable v1 contracts')
    commands.add_parser('check', help='Read-only check of topic configuration and registered contracts')
    args = parser.parse_args()
    try:
        if args.command == 'validate':
            payload = decode_json(args.path.read_text(encoding='utf-8-sig'))
            events = payload if isinstance(payload, list) else [payload]
            if not events:
                raise ValueError('An event array must not be empty')
            results = [{'index': index, 'errors': validation_errors(event)}
                       for index, event in enumerate(events)]
            ok = not any(result['errors'] for result in results)
            print(json.dumps({'valid': ok, 'events': results}, indent=2))
            return 0 if ok else 1
        from journeypulse.bootstrap import run
        print(json.dumps(run(create=args.command == 'bootstrap'), indent=2))
        return 0
    except (OSError, ValueError) as error:
        print(json.dumps({'status': 'failed', 'error': str(error)}), file=sys.stderr)
        return 1
    except Exception as error:
        # Avoid echoing connection strings/passwords from client exceptions.
        print(json.dumps({'status': 'failed', 'error_type': type(error).__name__,
                          'hint': 'Check Kafka/PostgreSQL readiness, credentials and JourneyPulse configuration.'}),
              file=sys.stderr)
        return 1


if __name__ == '__main__':
    raise SystemExit(main())
