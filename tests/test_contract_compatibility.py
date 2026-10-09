import json
from decimal import Decimal
from pathlib import Path
import unittest

from jsonschema import Draft202012Validator
from event_contract import decode_event

ROOT = Path(__file__).resolve().parents[1]


class ContractCompatibilityTests(unittest.TestCase):
    def test_frozen_v1_examples_match_schema_and_runtime(self):
        # Decimal prevents the schema validator itself introducing float cents errors.
        schema = json.loads((ROOT / 'contracts/events-v1.schema.json').read_text(), parse_float=Decimal)
        Draft202012Validator.check_schema(schema)
        validator = Draft202012Validator(schema)
        for fixture in json.loads((ROOT / 'contracts/v1-compatibility.json').read_text()):
            with self.subTest(fixture=fixture['name']):
                raw = json.dumps(fixture['event']).encode()
                row = decode_event(raw)
                self.assertEqual(row[7], fixture['error'])
                self.assertEqual(validator.is_valid(json.loads(raw, parse_float=Decimal)),
                                 fixture['error'] is None)
                if row[7] is None:
                    event = fixture['event']
                    self.assertEqual(row[:6], (event['event_id'], event['user_id'],
                        event['event_type'], event['timestamp'], Decimal(str(event['amount'])), event['currency']))

    def test_explicit_invalid_versions_preserve_payload(self):
        fixture = json.loads((ROOT / 'contracts/v1-compatibility.json').read_text())[0]['event']
        for version in (None, True, False, '1', 1.0, 0, 2, [], {}):
            with self.subTest(version=version):
                raw = json.dumps(dict(fixture, schema_version=version)).encode()
                self.assertEqual(decode_event(raw)[7], 'UNSUPPORTED_SCHEMA_VERSION')

    def test_runtime_retains_stricter_json_rules(self):
        self.assertEqual(decode_event(b'{"schema_version":1,"schema_version":1}')[7], 'MALFORMED_JSON')


if __name__ == '__main__':
    unittest.main()
