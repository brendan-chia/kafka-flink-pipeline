"""Validate schema/migration/reconciliation in an isolated, rolled-back schema."""
import subprocess
from pathlib import Path
from uuid import uuid4

root = Path(__file__).resolve().parents[1]
schema = 'event_time_test_' + uuid4().hex


def without_transaction(text):
    return '\n'.join(line for line in text.splitlines() if line.strip() not in (
        'BEGIN;', 'BEGIN ISOLATION LEVEL REPEATABLE READ;', 'COMMIT;'))


sql = f'BEGIN; CREATE SCHEMA {schema}; SET LOCAL search_path TO {schema};\n'
sql += (root / 'sql/init.sql').read_text(encoding='utf-8') + '\n'
sql += without_transaction((root / 'sql/migrations/002_event_time_business.sql').read_text(encoding='utf-8'))
sql += '''
INSERT INTO processed_events (event_id,user_id,event_type,amount,currency,
  event_timestamp_ms,ingested_at,source_topic,source_partition,source_offset)
VALUES ('first','user','payment',10.25,'MYR',1790985601000,'2026-10-03','test',0,0),
       ('late','user','payment',99.00,'MYR',1790985602000,'2026-10-03','test',0,1),
       ('usd','user','payment',3.33,'USD',1790985603000,'2026-10-03','test',0,2),
       ('food','user','food_order',20.00,'MYR',1790985604000,'2026-10-03','test',0,3);
INSERT INTO payment_revenue_windows VALUES
  ('2026-10-03 00:00:00','2026-10-03 00:05:00','MYR',1,10.25);
'''
sql += without_transaction((root / 'sql/reconcile_event_time_windows.sql').read_text(encoding='utf-8'))
sql += '''
DO $$ BEGIN
  IF (SELECT revenue FROM payment_revenue_windows WHERE currency='MYR') <> 109.25
     OR (SELECT payment_count FROM payment_revenue_windows WHERE currency='MYR') <> 2
     OR (SELECT revenue FROM payment_revenue_windows WHERE currency='USD') <> 3.33
     OR (SELECT count(*) FROM activity_windows) <> 3 THEN
    RAISE EXCEPTION 'Reconciliation does not match expected late-inclusive totals';
  END IF;
END $$;
ROLLBACK;
\echo PASS: fresh schema, repeatable migration and late-data reconciliation; all test changes rolled back.
'''
subprocess.run(['docker', 'exec', '-i', 'postgres', 'psql', '-v', 'ON_ERROR_STOP=1',
                '-v', 'from_utc=2026-10-03 00:00:00', '-v', 'until_utc=2026-10-03 01:00:00',
                '-U', 'grabuser', '-d', 'grabevents'], input=sql, text=True, check=True)
