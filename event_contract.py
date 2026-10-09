"""Dependency-free decoding shared by the Flink job and correctness tests."""

import base64
import json
from decimal import Decimal, InvalidOperation
from datetime import datetime, timezone

SCHEMA_VERSION = 1

CATEGORY_MAP = {
    'food_order': 'FOOD', 'ride_request': 'TRANSPORT',
    'payment': 'FINANCE', 'grocery_order': 'GROCERY',
}


def epoch_milliseconds(timestamp):
    """Convert a received timestamp exactly; naive Flink values are UTC."""
    if timestamp.tzinfo is None:
        timestamp = timestamp.replace(tzinfo=timezone.utc)
    delta = timestamp.astimezone(timezone.utc) - datetime(1970, 1, 1, tzinfo=timezone.utc)
    return (delta.days * 86400 + delta.seconds) * 1000 + delta.microseconds // 1000


def _reject_constant(value):
    raise ValueError(f'Non-finite JSON number: {value}')


def _unique_object(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError(f'Duplicate JSON field: {key}')
        result[key] = value
    return result


def _valid_identifier(value, limit):
    if not isinstance(value, str) or not value.strip() or len(value) > limit or '\x00' in value:
        return False
    try:
        value.encode('utf-8')
    except UnicodeEncodeError:
        return False
    return True


def decode_event(payload, received_timestamp_ms=None, max_future_skew_ms=60000):
    """Return exactly one normalized row, including errors and lossless raw bytes.

    Row order: event_id, user_id, event_type, event_timestamp_ms, amount,
    currency, category, error_reason, validation_errors, raw_payload_base64.
    Invalid rows have null business columns and never reach the JDBC sink.
    """
    raw = bytes(payload) if payload is not None else b''
    encoded = base64.b64encode(raw).decode('ascii')

    def invalid(errors):
        reason = errors[0] if len(errors) == 1 else 'MULTIPLE_ERRORS'
        return (None,) * 7 + (reason, json.dumps(errors), encoded)

    if payload is None:
        return invalid(['NULL_PAYLOAD'])
    try:
        event = json.loads(raw.decode('utf-8'), parse_float=Decimal,
                           parse_constant=_reject_constant, object_pairs_hook=_unique_object)
    except (UnicodeDecodeError, ValueError, RecursionError):
        return invalid(['MALFORMED_JSON'])
    if not isinstance(event, dict):
        return invalid(['INVALID_JSON_OBJECT'])

    errors = []
    # Unversioned retained events are v1; explicit unknown versions are quarantined.
    version = event.get('schema_version', SCHEMA_VERSION)
    if type(version) is not int or version != SCHEMA_VERSION:
        errors.append('UNSUPPORTED_SCHEMA_VERSION')
    for field, limit in [('event_id', 100), ('user_id', 100)]:
        value = event.get(field)
        if not _valid_identifier(value, limit):
            errors.append('INVALID_' + field.upper())
    event_type = event.get('event_type')
    if not isinstance(event_type, str) or event_type not in CATEGORY_MAP:
        errors.append('INVALID_EVENT_TYPE')
    timestamp = event.get('timestamp')
    if type(timestamp) is not int or not 0 < timestamp <= 253402300799999:
        errors.append('INVALID_TIMESTAMP')
    elif received_timestamp_ms is not None and timestamp > received_timestamp_ms + max_future_skew_ms:
        errors.append('FUTURE_EVENT_TIME')
    currency = event.get('currency')
    if not isinstance(currency, str) or len(currency) != 3 or not currency.isascii() or not currency.isalpha() or currency != currency.upper():
        errors.append('INVALID_CURRENCY')
    amount = event.get('amount')
    try:
        if isinstance(amount, bool) or not isinstance(amount, (int, Decimal)):
            raise ValueError('amount must be a JSON number')
        amount = Decimal(amount)
        if not amount.is_finite() or amount < 0 or amount > Decimal('99999999.99'):
            errors.append('INVALID_AMOUNT')
        elif amount != amount.quantize(Decimal('0.01')):
            errors.append('INVALID_AMOUNT_PRECISION')
        else:
            amount = amount.quantize(Decimal('0.01'))
    except (InvalidOperation, ValueError, OverflowError):
        errors.append('INVALID_AMOUNT')
    if errors:
        return invalid(errors)
    return (event['event_id'], event['user_id'], event_type, timestamp, amount,
            currency, CATEGORY_MAP[event_type], None, '[]', encoded)
