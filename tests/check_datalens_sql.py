"""Exercise exact diagnostic SQL and migration on PostgreSQL; roll back all writes."""
import argparse
from datetime import datetime, timezone
from pathlib import Path
import subprocess
import sys
from uuid import uuid4
from unittest.mock import MagicMock, patch

import psycopg2
from psycopg2.extensions import adapt
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from datalens import evidence


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--container', default='postgres')
    args = parser.parse_args()
    root = Path(__file__).resolve().parents[1]
    schema = 'datalens_test_' + uuid4().hex
    catalogue = schema + '_evidence'
    migration = (root / 'sql/migrations/004_datalens_evidence.sql').read_text()
    migration = migration.replace('BEGIN;', '').replace('COMMIT;', '').replace('datalens.', catalogue + '.').replace('SCHEMA IF NOT EXISTS datalens', 'SCHEMA IF NOT EXISTS ' + catalogue)
    sql = 'BEGIN; CREATE SCHEMA ' + schema + '; SET search_path TO ' + schema + "; SET TIME ZONE 'UTC';"
    sql += (root / 'sql/init.sql').read_text() + migration + migration
    sql += "DO $$ BEGIN IF (SELECT count(*) FROM " + catalogue + ".metrics) <> 2 THEN RAISE EXCEPTION 'Catalogue seed wrong'; END IF; END $$;"
    # Fixed, closed window: payment 0.10 + 0.20, fresh food, and another currency.
    sql += """INSERT INTO processed_events
        (event_id,user_id,event_type,amount,currency,event_timestamp_ms,ingested_at,processed_at,source_topic,source_partition,source_offset)
        VALUES ('a','u','payment',0.10,'MYR',1767225600000,'2026-01-01','2026-01-01','test',0,0),
               ('b','u','payment',0.20,'MYR',1767225601000,'2026-01-02','2026-01-02','test',0,1),
               ('usd','u','payment',99,'USD',1767225600000,'2026-01-01','2026-01-01','test',0,2),
               ('food','u','food_order',500,'MYR',1767225600000,CURRENT_TIMESTAMP,CURRENT_TIMESTAMP,'test',0,3);
        INSERT INTO payment_revenue_windows VALUES ('2026-01-01','2026-01-01 00:05', 'MYR',1,0.10);
        """
    # Capture the real function's query and bound arguments; adapt only fixed test data.
    conn = MagicMock()
    cur = conn.cursor.return_value.__enter__.return_value
    cur.fetchone.return_value = {'observed_at':datetime.now(timezone.utc)}
    cur.fetchall.return_value = []
    with patch.object(evidence,'connect',return_value=conn):
        evidence.compare_revenue({},'2026-01-01T00:00:00Z','2026-01-01T00:05:00Z')
    query, params = cur.execute.call_args.args
    for value in params:
        query = query.replace('%s',adapt(value).getquoted().decode(),1)
    sql += 'CREATE TEMP TABLE comparison AS ' + query + ';'
    sql += """DO $$ BEGIN IF (SELECT count(*) FROM comparison) <> 1
        OR (SELECT audit_count <> 2 OR audit_revenue <> 0.30 OR stored_count <> 1 OR stored_revenue <> 0.10 FROM comparison)
        THEN RAISE EXCEPTION 'Late payment/currency comparison wrong'; END IF; END $$;"""
    sql += 'CREATE TEMP TABLE freshness AS ' + evidence.FRESHNESS_SQL + ';'
    sql += """DO $$ BEGIN IF (SELECT payment_count <> 3 OR latest_processed_at <> '2026-01-02'::timestamp FROM freshness)
        THEN RAISE EXCEPTION 'Food activity contaminated payment freshness'; END IF; END $$;"""
    sql += 'INSERT INTO ' + catalogue + """.quality_results VALUES ('00000000-0000-0000-0000-000000000001',CURRENT_TIMESTAMP,'source_validation_window','user-events','partial',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP,'{"valid_deliveries":2,"invalid_deliveries":1}');"""
    sql += "DO $$ BEGIN IF (SELECT evidence->>'invalid_deliveries' FROM " + catalogue + ".quality_results) <> '1' THEN RAISE EXCEPTION 'Evidence persistence wrong'; END IF; END $$; ROLLBACK;"
    subprocess.run(['docker','exec','-i',args.container,'psql','-v','ON_ERROR_STOP=1','-U','grabuser','-d','grabevents'],input=sql,text=True,check=True)
    print('PASS: repeatable catalogue migration, exact-money/currency/late-payment diagnostics, payment freshness and persisted JSON evidence; all writes rolled back.')


if __name__ == '__main__':
    main()
