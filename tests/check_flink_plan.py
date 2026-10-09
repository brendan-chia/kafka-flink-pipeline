"""Validate actual connector DDL and both routing branches without a broker."""
import importlib.util
from pathlib import Path

path = Path(__file__).resolve().parents[1] / 'flink-processor' / 'pipeline.py'
spec = importlib.util.spec_from_file_location('pipeline', path)
pipeline = importlib.util.module_from_spec(spec)
spec.loader.exec_module(pipeline)
env = pipeline.create_table_env()
pipeline.create_source_table(env)
pipeline.create_postgres_sink(env)
pipeline.create_dlq_sink(env)
pipeline.create_business_views(env)
pipeline.create_business_sinks(env)
plan = pipeline.build_statement_set(env).explain()
assert 'processed_events' in plan and 'dlq_events' in plan, plan
assert 'payment_revenue_windows' in plan and 'activity_windows' in plan, plan
print('PASS: Kafka source, validation, audit/DLQ and both event-time business sink plans compile.')
