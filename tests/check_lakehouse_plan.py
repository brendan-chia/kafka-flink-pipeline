"""Compile the actual seven-output plan against the running REST/MinIO catalog.

Requires Java 17 and verified lakehouse JARs. Does not connect to live Kafka or
PostgreSQL; the catalog DDL creates small tables in an isolated test namespace.
"""
import importlib.util
import os
from pathlib import Path
from uuid import uuid4

os.environ['LAKEHOUSE_ENABLED'] = '1'
os.environ.setdefault('LAKEHOUSE_NAMESPACE', 'check_plan_' + uuid4().hex[:12])
root = Path(__file__).resolve().parents[1]
spec = importlib.util.spec_from_file_location('pipeline', root / 'flink-processor/pipeline.py')
pipeline = importlib.util.module_from_spec(spec)
spec.loader.exec_module(pipeline)
import lakehouse_config

env = pipeline.create_table_env()
pipeline.create_source_table(env)
pipeline.create_postgres_sink(env)
pipeline.create_dlq_sink(env)
pipeline.create_business_views(env)
pipeline.create_business_sinks(env)
lakehouse_config.create_catalog(env)
lakehouse_config.create_sinks(env)
plan = pipeline.build_statement_set(env).explain()
for table in ('processed_events','dlq_events','payment_revenue_windows','activity_windows',
              'event_history','validated_events','revenue_finalized'):
    assert table in plan, (table,plan)
print('PASS: real seven-output Flink plan and REST/MinIO catalog DDL compile.')
print('Test namespace: ' + lakehouse_config.namespace())
