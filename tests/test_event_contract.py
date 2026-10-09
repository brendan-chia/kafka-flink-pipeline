import base64
import json
import unittest
from decimal import Decimal
from datetime import datetime, timedelta, timezone

from event_contract import decode_event, epoch_milliseconds


class EventContractTests(unittest.TestCase):
    def setUp(self):
        self.event = {'event_id': 'test-payment-1', 'user_id': 'user_1',
                      'event_type': 'payment', 'timestamp': 1787486400000,
                      'amount': 24.50, 'currency': 'MYR'}

    def decode(self, event):
        return decode_event(json.dumps(event).encode())

    def test_valid_money_and_identity_survive_replay(self):
        first = self.decode(self.event)
        self.assertEqual(first, self.decode(self.event))
        self.assertEqual(first[:7], ('test-payment-1', 'user_1', 'payment',
                                     1787486400000, Decimal('24.50'), 'MYR', 'FINANCE'))
        self.assertIsNone(first[7])

    def test_each_missing_or_null_required_field_routes_to_dlq(self):
        for field in self.event:
            for missing in (True, False):
                with self.subTest(field=field, missing=missing):
                    event = self.event.copy()
                    if missing:
                        del event[field]
                    else:
                        event[field] = None
                    row = self.decode(event)
                    self.assertIsNotNone(row[7])
                    self.assertIn('INVALID_' + field.upper(), json.loads(row[8]))

    def test_multiple_errors_are_reported_together(self):
        self.event.update(user_id=None, event_type=None, amount=None)
        row = self.decode(self.event)
        self.assertEqual(row[7], 'MULTIPLE_ERRORS')
        self.assertEqual(json.loads(row[8]), ['INVALID_USER_ID', 'INVALID_EVENT_TYPE', 'INVALID_AMOUNT'])

    def test_malformed_bytes_are_preserved_losslessly(self):
        for payload in (b'{broken', b'\xff\x00', b'NaN', b'Infinity', b'', b'{"amount":1,"amount":2}'):
            with self.subTest(payload=payload):
                row = decode_event(payload)
                self.assertEqual(row[7], 'MALFORMED_JSON')
                self.assertEqual(base64.b64decode(row[9]), payload)

    def test_tombstone_routes_to_dlq(self):
        self.assertEqual(decode_event(None)[7], 'NULL_PAYLOAD')

    def test_non_object_json_routes_to_dlq(self):
        for payload in (b'null', b'[]', b'42', b'"text"'):
            self.assertEqual(decode_event(payload)[7], 'INVALID_JSON_OBJECT')

    def test_money_rejects_wrong_types_range_and_precision(self):
        for amount in (True, '12.50', {}, [], -0.01, 100000000, 1.001):
            with self.subTest(amount=amount):
                self.event['amount'] = amount
                self.assertIsNotNone(self.decode(self.event)[7])

    def test_money_accepts_boundaries(self):
        for amount in (0, 99999999.99, 0.01):
            self.event['amount'] = amount
            self.assertIsNone(self.decode(self.event)[7])

    def test_identifiers_reject_unsafe_or_oversized_values(self):
        for field in ('event_id', 'user_id'):
            for value in ('', ' ', 'x' * 101, 'x\x00', '\ud800', 42, {}):
                event = self.event.copy()
                event[field] = value
                self.assertIn('INVALID_' + field.upper(), json.loads(self.decode(event)[8]))

    def test_timestamp_and_currency_types(self):
        for timestamp in (True, '123', 1.5, 0, -1, 253402300800000):
            event = dict(self.event, timestamp=timestamp)
            self.assertEqual(self.decode(event)[7], 'INVALID_TIMESTAMP')
        for currency in ('myr', 'MY', 'MYRR', '123', 'ＭＹＲ', True):
            event = dict(self.event, currency=currency)
            self.assertEqual(self.decode(event)[7], 'INVALID_CURRENCY')

    def test_future_timestamp_cannot_poison_watermarks(self):
        received = self.event['timestamp']
        event = dict(self.event, timestamp=received + 60000)
        self.assertIsNone(decode_event(json.dumps(event).encode(), received)[7])
        event['timestamp'] += 1
        row = decode_event(json.dumps(event).encode(), received)
        self.assertEqual(row[7], 'FUTURE_EVENT_TIME')
        self.assertIsNone(row[3])

    def test_received_time_uses_exact_milliseconds(self):
        epoch = datetime(1970, 1, 1, tzinfo=timezone.utc)
        for milliseconds in (1790985600123, 1790985600690, 1790985600999):
            value = epoch + timedelta(milliseconds=milliseconds)
            self.assertEqual(epoch_milliseconds(value), milliseconds)
            self.assertEqual(epoch_milliseconds(value.replace(tzinfo=None)), milliseconds)
            self.assertEqual(epoch_milliseconds(value.astimezone(timezone(timedelta(hours=8)))), milliseconds)


if __name__ == '__main__':
    unittest.main()
