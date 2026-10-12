"""Local contract loading and validation; no network or registry required."""
from copy import deepcopy
import json
from pathlib import Path

from jsonschema import Draft202012Validator, FormatChecker
from rfc3339_validator import validate_rfc3339

ROOT = Path(__file__).resolve().parents[1]
CONTRACT_PATH = ROOT / 'contracts/journeypulse/events-v1.schema.json'
EVENT_NAMES = ('session_started', 'restaurant_view', 'add_to_cart',
               'checkout_started', 'order_completed')
SCHEMA_VERSION = 1
FORMAT_CHECKER = FormatChecker()


@FORMAT_CHECKER.checks('date-time')
def _date_time(value):
    # Explicit dependency avoids silently skipping date-time checks.
    return not isinstance(value, str) or (value == value.strip() and validate_rfc3339(value.upper()))


def load_schema():
    schema = json.loads(CONTRACT_PATH.read_text(encoding='utf-8'))
    Draft202012Validator.check_schema(schema)
    return schema


def event_schema(event_name):
    """A self-contained registry definition restricted to one event name."""
    if event_name not in EVENT_NAMES:
        raise ValueError('Unknown JourneyPulse event name: ' + event_name)
    schema = deepcopy(load_schema())
    schema['properties']['event_name'] = {'const': event_name}
    return schema


def validation_errors(event, validator=None):
    validator = validator if validator is not None else Draft202012Validator(load_schema(), format_checker=FORMAT_CHECKER)
    errors = sorted(validator.iter_errors(event),
                    key=lambda error: (tuple(str(p) for p in error.absolute_path), error.message))
    return [{'path': '$' + ''.join(f'[{p}]' if isinstance(p, int) else f'.{p}'
                                 for p in error.absolute_path),
             'message': error.message, 'rule': error.validator} for error in errors]


def _unique_keys(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError('Duplicate JSON field: ' + key)
        result[key] = value
    return result


def _reject_constant(value):
    raise ValueError('Non-finite JSON number: ' + value)


def decode_json(payload):
    """Reject duplicate keys, NaN and Infinity before applying the contract."""
    return json.loads(payload, object_pairs_hook=_unique_keys,
                      parse_constant=_reject_constant)
