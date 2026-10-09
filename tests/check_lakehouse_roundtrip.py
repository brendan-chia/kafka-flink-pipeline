"""Actual Flink -> REST catalog -> MinIO -> Trino smoke and correction test.

Requires the running optional lakehouse and Java 17. Creates a unique test
namespace and retains its small tables so results can be inspected afterward.
Uses bounded fixtures, not live Kafka. Does not touch operational PostgreSQL.
"""
from datetime import datetime, timezone
from decimal import Decimal
import importlib.util
import json
import os
from pathlib import Path
import sys
from uuid import uuid4

root = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(root))
os.environ['LAKEHOUSE_ENABLED'] = '1'
os.environ['LAKEHOUSE_NAMESPACE'] = 'check_' + uuid4().hex[:12]
spec = importlib.util.spec_from_file_location('pipeline', root / 'flink-processor/pipeline.py')
pipeline = importlib.util.module_from_spec(spec)
spec.loader.exec_module(pipeline)
import lakehouse_config
from lakehouse_ops import TrinoClient, canonical_cte, reconciliation_sql, replay_sql
from scripts.lakehouse import bootstrap, replay_payload
from pyflink.table import DataTypes

base = 1577836800000
event_a = dict(event_id='a',user_id='user',event_type='payment',timestamp=base+1000,amount=10.10,currency='MYR')
event_b = dict(event_id='b',user_id='user',event_type='payment',timestamp=base+2000,amount=0.20,currency='MYR')
payloads = [json.dumps(event_a).encode(),json.dumps(event_a).encode(),json.dumps(event_b).encode(),
            b'{broken',b'\xff',b'',None]
received = datetime(2020,1,1,0,1)
raw_rows = [(bytearray(payload) if payload is not None else None,
             'test-input',0,index,received,received) for index,payload in enumerate(payloads)]
schema = DataTypes.ROW([
    DataTypes.FIELD('raw_payload',DataTypes.BYTES()),
    DataTypes.FIELD('source_topic',DataTypes.STRING()),
    DataTypes.FIELD('source_partition',DataTypes.INT()),
    DataTypes.FIELD('source_offset',DataTypes.BIGINT()),
    DataTypes.FIELD('ingested_at',DataTypes.TIMESTAMP(3)),
    DataTypes.FIELD('event_time',DataTypes.TIMESTAMP(3)),
])
env = pipeline.create_table_env()
env.get_config().set('restart-strategy.type','none')
env.create_temporary_view('raw_events',env.from_elements(raw_rows,schema))
pipeline.create_validation_view(env)
lakehouse_config.create_catalog(env)
lakehouse_config.create_sinks(env)
prefix = f'lakehouse.{lakehouse_config.namespace()}'

# Append twice to demonstrate fresh replay duplication and logical deduplication.
revenue = """SELECT TIMESTAMP '2020-01-01 00:00:00' AS window_start,
    TIMESTAMP '2020-01-01 00:05:00' AS window_end,'MYR' AS currency,
    CAST(2 AS BIGINT) AS payment_count,CAST(10.30 AS DECIMAL(38,2)) AS revenue"""
for _ in range(2):
    statements = env.create_statement_set()
    lakehouse_config.add_inserts(statements,revenue)
    statements.execute().wait()

client = TrinoClient(os.getenv('TRINO_URL','http://localhost:18080'))
history = client.current_snapshot('event_history')
validated = client.current_snapshot('validated_events')
assert client.query(f'SELECT count(*) AS n FROM {prefix}.event_history')[0]['n'] == 14
assert client.query(f'SELECT count(*) AS n FROM {prefix}.validated_events')[0]['n'] == 6
assert client.query(canonical_cte(validated)+' SELECT count(*) AS n FROM canonical')[0]['n'] == 2
restored = client.query(replay_sql(history,base,base+300000,100))
assert [replay_payload(row) for row in restored] == payloads

bootstrap(client)
merge = reconciliation_sql(validated,base,base+300000,300)
client.query(merge)
client.query(merge)
result = client.query(f'SELECT payment_count,revenue FROM {prefix}.revenue_reconciled')
assert result[0]['payment_count'] == 2 and Decimal(str(result[0]['revenue'])) == Decimal('10.30'),result

# A stale group must be removed by the same atomic MERGE, not left behind.
client.query(f'''INSERT INTO {prefix}.revenue_reconciled VALUES
    (TIMESTAMP '2020-01-01 00:00:00',TIMESTAMP '2020-01-01 00:05:00','USD',1,999,0,CAST(current_timestamp AS timestamp(3)))''')
client.query(merge)
assert client.query(f'SELECT count(*) AS n FROM {prefix}.revenue_reconciled')[0]['n'] == 1

# Add a late valid event through the real Flink Iceberg sink. The old pinned
# snapshot must still produce the original total; a new snapshot corrects it.
event_c = dict(event_a,event_id='late',timestamp=base+3000,amount=1.00)
env.drop_temporary_view('validated_events')
env.drop_temporary_view('raw_events')
late_rows = [(bytearray(json.dumps(event_c).encode()),'test-input',0,7,received,received)]
env.create_temporary_view('raw_events',env.from_elements(late_rows,schema))
pipeline.create_validation_view(env)
statements = env.create_statement_set()
lakehouse_config.add_inserts(statements,revenue)
statements.execute().wait()
client.query(merge)
assert Decimal(str(client.query(f'SELECT revenue FROM {prefix}.revenue_reconciled')[0]['revenue'])) == Decimal('10.30')
new_snapshot = client.current_snapshot('validated_events')
client.query(reconciliation_sql(new_snapshot,base,base+300000,300))
assert Decimal(str(client.query(f'SELECT revenue FROM {prefix}.revenue_reconciled')[0]['revenue'])) == Decimal('11.30')
report = {'namespace':lakehouse_config.namespace(),'history_snapshot':history,
          'original_validated_snapshot':validated,'late_validated_snapshot':new_snapshot,
          'status':'passed','transport':'bounded Flink fixtures -> REST/MinIO -> Trino',
          'checked_at_utc':datetime.now(timezone.utc).isoformat()}
output = root / '.flink-state/lakehouse' / f"{report['namespace']}.json"
output.parent.mkdir(parents=True,exist_ok=True)
output.write_text(json.dumps(report,indent=2))
print('PASS: binary/tombstone history, replay deduplication, atomic currency repair, snapshot isolation and late-inclusive correction.')
print(f'Test tables retained in {prefix}; report: {output}')
