"""Build the actual PyFlink graph without contacting Kafka/PostgreSQL."""
import json
from pathlib import Path
import sys
from unittest.mock import MagicMock, patch
from pyflink.datastream import RuntimeContext
from pyflink.datastream.data_stream import DataStream
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from journeypulse.processor import build_job

if __name__ == '__main__':
    # Opening the real writer against the installed API catches runtime-context
    # incompatibilities that graph construction alone cannot find. No DB IO occurs.
    writers = []
    original_map = DataStream.map
    def capture(stream, function, *args, **kwargs):
        if function.__class__.__name__ == 'Persist':
            writers.append(function)
        return original_map(stream, function, *args, **kwargs)
    with patch.object(DataStream, 'map', capture):
        environment = build_job()
    assert len(writers) == 1
    context = MagicMock(spec=RuntimeContext)
    writers[0].open(context)
    assert set(writers[0].counters) == {'applied', 'duplicate', 'conflict', 'invalid', 'replay'}
    assert context.get_metrics_group.return_value.counter.call_count == 5
    assert environment.get_checkpoint_config().get_checkpoint_interval() == 10000
    plan = json.loads(environment.get_execution_plan())
    assert plan['nodes'], plan
    writers = [node for node in plan['nodes'] if node['type'] == 'journeypulse-profile-writes']
    assert len(writers) == 1, plan
    assert any('metadata=[topic, partition, offset]' in node['contents'] for node in plan['nodes']), plan
    print(json.dumps({'status': 'planned', 'operators': len(plan['nodes'])}))
