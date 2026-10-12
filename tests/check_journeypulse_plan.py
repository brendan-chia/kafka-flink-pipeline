"""Build the actual PyFlink graph without contacting Kafka/PostgreSQL."""
import json
from pathlib import Path
import sys
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from journeypulse.processor import build_job

if __name__ == '__main__':
    environment = build_job()
    assert environment.get_checkpoint_config().get_checkpoint_interval() == 10000
    plan = json.loads(environment.get_execution_plan())
    assert plan['nodes'], plan
    writers = [node for node in plan['nodes'] if node['type'] == 'journeypulse-profile-writes']
    assert len(writers) == 1, plan
    assert any('metadata=[topic, partition, offset]' in node['contents'] for node in plan['nodes']), plan
    print(json.dumps({'status': 'planned', 'operators': len(plan['nodes'])}))
