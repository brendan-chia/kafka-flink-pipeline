"""Independent PyFlink job: raw Kafka metadata -> transactional profile store."""
import os
from pathlib import Path
import time

from .config import EVENT_TOPIC, Settings


def source_ddl(bootstrap, group):
    def literal(value):
        return value.replace("'", "''")
    return f"""CREATE TABLE journey_events (
        user_key STRING,
        payload STRING,
        topic STRING METADATA FROM 'topic' VIRTUAL,
        partition_id INT METADATA FROM 'partition' VIRTUAL,
        offset_id BIGINT METADATA FROM 'offset' VIRTUAL
    ) WITH (
        'connector'='kafka', 'topic'='{EVENT_TOPIC}',
        'properties.bootstrap.servers'='{literal(bootstrap)}',
        'properties.group.id'='{literal(group)}',
        'properties.enable.auto.commit'='false',
        'scan.startup.mode'='earliest-offset',
        'key.format'='raw', 'key.fields'='user_key',
        'value.format'='raw', 'value.fields-include'='EXCEPT_KEY'
    )"""


def configuration():
    state = Path(__file__).resolve().parents[1] / '.journeypulse-state'
    values = {
        'execution.checkpointing.interval': '10 s',
        'execution.checkpointing.mode': 'EXACTLY_ONCE',
        'execution.checkpointing.storage': 'filesystem',
        'execution.checkpointing.dir': os.getenv('JOURNEYPULSE_CHECKPOINT_DIR', (state / 'checkpoints').as_uri()),
        'execution.checkpointing.savepoint-dir': os.getenv('JOURNEYPULSE_SAVEPOINT_DIR', (state / 'savepoints').as_uri()),
        'execution.checkpointing.externalized-checkpoint-retention': 'RETAIN_ON_CANCELLATION',
        'execution.checkpointing.timeout': '2 min',
        'execution.checkpointing.max-concurrent-checkpoints': '1',
        'restart-strategy.type': 'fixed-delay',
        'restart-strategy.fixed-delay.attempts': '10',
        'restart-strategy.fixed-delay.delay': '10 s',
        'table.local-time-zone': 'UTC',
        'python.fn-execution.bundle.size': '1',
    }
    if os.getenv('FLINK_PYTHON_EXECUTABLE'):
        values['python.executable'] = os.environ['FLINK_PYTHON_EXECUTABLE']
    if os.getenv('JOURNEYPULSE_RESTORE_PATH'):
        values['execution.state-recovery.path'] = os.environ['JOURNEYPULSE_RESTORE_PATH']
        values['execution.state-recovery.ignore-unclaimed-state'] = 'false'
        values['execution.state-recovery.claim-mode'] = 'NO_CLAIM'
    return values


def build_job():
    from pyflink.common import Configuration, Types
    from pyflink.datastream import StreamExecutionEnvironment
    from pyflink.datastream.functions import MapFunction
    from pyflink.table import StreamTableEnvironment
    from .profiles import ProfileStore

    class Persist(MapFunction):
        def open(self, runtime_context):
            self.store = ProfileStore()
            self.next_prune = 0
            self.counters = {name: runtime_context.get_metric_group().counter(name)
                             for name in ('applied', 'duplicate', 'conflict', 'invalid', 'replay')}

        def map(self, row):
            # Any database failure propagates: source offsets must not checkpoint past it.
            if time.monotonic() >= self.next_prune:
                self.store.prune_event_ids()
                self.next_prune = time.monotonic() + 60
            key, payload, topic, partition, offset = row
            outcome = self.store.apply(topic, partition, offset, key, payload)
            self.counters[outcome].inc()
            return outcome

    config = Configuration()
    for name, value in configuration().items():
        config.set_string(name, value)
    env = StreamExecutionEnvironment.get_execution_environment(config)
    parallelism = int(os.getenv('JOURNEYPULSE_PARALLELISM', '1'))
    if parallelism < 1:
        raise ValueError('JourneyPulse parallelism must be positive')
    env.set_parallelism(parallelism)
    jar = Path(os.getenv('JOURNEYPULSE_KAFKA_JAR', str(Path(__file__).resolve().parents[1] /
        'jars/flink-sql-connector-kafka-4.0.1-2.0.jar'))).resolve()
    if not jar.is_file():
        raise FileNotFoundError('Kafka connector missing; run download_jars.sh')
    env.add_jars(jar.as_uri())
    table_env = StreamTableEnvironment.create(env)
    group = os.getenv('JOURNEYPULSE_CONSUMER_GROUP', 'journeypulse-profiles-v1')
    if not group or group == 'flink-grab-consumer':
        raise ValueError('JourneyPulse requires its own consumer group')
    table_env.execute_sql(source_ddl(Settings.from_environment().kafka_bootstrap, group))
    stream = table_env.to_data_stream(table_env.from_path('journey_events'))
    stream.map(Persist(), output_type=Types.STRING()).name('journeypulse-profile-writes').uid(
        'journeypulse-profile-writes-v1').print().name('journeypulse-outcomes')
    return env


def main():
    build_job().execute('JourneyPulse profiles v1')
