"""Execute the Python table function against bounded fixtures (no Kafka needed)."""
import base64
import json
from check_flink_plan import pipeline, env
from pyflink.table import DataTypes

valid = json.dumps({'event_id': 'runtime-test', 'user_id': 'user_1',
                    'event_type': 'payment', 'timestamp': 1787486400000,
                    'amount': 12.50, 'currency': 'MYR'}).encode()
future = json.dumps({'event_id': 'future-test', 'user_id': 'user_1',
                     'event_type': 'payment', 'timestamp': 253402300799999,
                     'amount': 1, 'currency': 'MYR'}).encode()
unsupported = json.dumps(dict(json.loads(valid), schema_version=2)).encode()
payloads = [valid, b'{broken', b'\xff', b'{}', None, future, unsupported]
fixtures = env.from_elements([(bytearray(payload) if payload is not None else None,) for payload in payloads],
                             DataTypes.ROW([DataTypes.FIELD('payload', DataTypes.BYTES())]))
env.create_temporary_view('fixtures', fixtures)
result = env.sql_query('''
    SELECT p.* FROM fixtures AS f,
    LATERAL TABLE(parse_event(f.payload, TO_TIMESTAMP_LTZ(1790985600000, 3))) AS p
''').execute()
with result.collect() as rows:
    actual = list(rows)
assert len(actual) == len(payloads), actual
assert sum(row[7] is None for row in actual) == 1, actual
assert sorted(row[7] for row in actual if row[7]) == [
    'FUTURE_EVENT_TIME', 'MALFORMED_JSON', 'MALFORMED_JSON', 'MULTIPLE_ERRORS', 'NULL_PAYLOAD',
    'UNSUPPORTED_SCHEMA_VERSION'], actual
assert sorted(row[9] for row in actual) == sorted(
    base64.b64encode(payload or b'').decode() for payload in payloads), actual
print('PASS: Flink executed one validator output per input, including null and invalid UTF-8.')
