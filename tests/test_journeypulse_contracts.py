"""Executable clickstream contract examples and compatibility boundaries."""
from copy import deepcopy
import json
from pathlib import Path
import subprocess
import sys
import unittest

from jsonschema import Draft202012Validator, FormatChecker
from event_contract import decode_event
from journeypulse.contracts import (ROOT, EVENT_NAMES, decode_json, event_schema,
                                   load_schema, validation_errors)

EXAMPLES = ROOT / 'contracts/journeypulse/examples'


class JourneyPulseContractTests(unittest.TestCase):
    def setUp(self):
        self.events = json.loads((EXAMPLES / 'valid-journey.json').read_text(encoding='utf-8'))

    def test_all_five_event_contracts_accept_their_examples(self):
        self.assertEqual({e['event_name'] for e in self.events}, set(EVENT_NAMES))
        for event in self.events:
            with self.subTest(event=event['event_name']):
                self.assertEqual(validation_errors(event), [])
                validator = Draft202012Validator(event_schema(event['event_name']),
                                                 format_checker=FormatChecker())
                self.assertTrue(validator.is_valid(event))
                other = next(e for e in self.events if e['event_name'] != event['event_name'])
                self.assertFalse(validator.is_valid(other))

    def test_cart_errors_include_paths_and_both_failures(self):
        errors = validation_errors(json.loads((EXAMPLES / 'invalid-add-to-cart.json').read_text()))
        self.assertTrue(any(e['path'] == '$.properties' and 'item_id' in e['message'] for e in errors))
        self.assertTrue(any(e['path'] == '$.properties.quantity' for e in errors))

    def test_every_event_requires_its_specific_properties(self):
        for original in self.events:
            for field in original['properties']:
                with self.subTest(event=original['event_name'], field=field):
                    event = deepcopy(original)
                    del event['properties'][field]
                    self.assertTrue(validation_errors(event))

    def test_required_envelope_and_unknown_fields(self):
        for key in self.events[0]:
            event = deepcopy(self.events[0])
            del event[key]
            with self.subTest(missing=key):
                self.assertTrue(validation_errors(event))
        for extra in ('amount', 'currency', 'unregistered_field'):
            with self.subTest(extra=extra):
                self.assertTrue(validation_errors(dict(self.events[0], **{extra: 1})))
        event = deepcopy(self.events[0])
        event['properties']['unknown'] = 'x'
        self.assertTrue(validation_errors(event))

    def test_unknown_event_and_version_are_rejected(self):
        for version in (None, True, False, '1', 0, 2):
            with self.subTest(version=version):
                self.assertTrue(validation_errors(dict(self.events[0], schema_version=version)))
        self.assertTrue(validation_errors(dict(self.events[0], event_name='payment')))

    def test_datetime_format_is_actually_enforced(self):
        for timestamp in ('yesterday', '2026-13-01T08:00:00Z', '2026-02-30T08:00:00Z',
                          '2026-10-12T08:00:00', '2026-10-12', '2026-10-12T08:00:00Z\n'):
            with self.subTest(timestamp=timestamp):
                self.assertTrue(validation_errors(dict(self.events[0], occurred_at=timestamp)))
        self.assertEqual(validation_errors(dict(self.events[0], occurred_at='2026-10-12T16:00:00+08:00')), [])

    def test_identifiers_and_quantity_boundaries(self):
        for identifier in ('', '   ', 'a' * 101, 'u\\x00', 'user name'):
            with self.subTest(identifier=identifier):
                self.assertTrue(validation_errors(dict(self.events[0], user_id=identifier)))
        for quantity in (0, -1, True, '1', 1.5, 1001):
            event = deepcopy(self.events[2])
            event['properties']['quantity'] = quantity
            with self.subTest(quantity=quantity):
                self.assertTrue(validation_errors(event))

    def test_wrong_property_shape_is_rejected(self):
        for properties in (None, [], 'x', 1):
            self.assertTrue(validation_errors(dict(self.events[0], properties=properties)))

    def test_decoder_rejects_ambiguous_and_non_json_payloads(self):
        for raw in ('{"event_id":"a","event_id":"b"}', '{"properties":{"quantity":NaN}}',
                    '{"x":Infinity}', '{"x":-Infinity}', '{bad', b'\\xff'):
            with self.subTest(raw=raw):
                with self.assertRaises((ValueError, UnicodeDecodeError)):
                    decode_json(raw)

    def test_clickstream_does_not_enter_transaction_contract(self):
        for event in self.events:
            self.assertIsNotNone(decode_event(json.dumps(event).encode())[7])
        self.assertTrue(validation_errors({'schema_version': 1, 'event_type': 'payment'}))

    def test_schema_is_valid_and_registry_rejects_unknown_names(self):
        Draft202012Validator.check_schema(load_schema())
        with self.assertRaises(ValueError):
            event_schema('payment')

    def test_cli_exit_codes_and_default_examples(self):
        script = str(ROOT / 'scripts/journeypulse.py')
        good = subprocess.run([sys.executable, script, 'validate'], capture_output=True, text=True)
        self.assertEqual(good.returncode, 0, good.stderr)
        self.assertTrue(json.loads(good.stdout)['valid'])
        bad = subprocess.run([sys.executable, script, 'validate',
                              str(EXAMPLES / 'invalid-add-to-cart.json')], capture_output=True, text=True)
        self.assertEqual(bad.returncode, 1)
        self.assertFalse(json.loads(bad.stdout)['valid'])


if __name__ == '__main__':
    unittest.main()
