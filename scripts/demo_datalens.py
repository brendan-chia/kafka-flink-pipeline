"""Operator-controlled, isolated late-payment evidence demonstration (not a streaming fault)."""
import argparse
import json
from pathlib import Path
import sys
from psycopg2 import OperationalError
from psycopg2.errors import InsufficientPrivilege, DuplicateDatabase
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from datalens import evidence
from datalens.benchmark import load_cases, NoModel
from datalens.evaluation_db import demo_name, setup_demo, require_owned, inject_late, repair_demo
from datalens.investigation import EvidenceService
from datalens.assistant_models import AssistantRequest
from datalens.llm import OpenAIModel
from datalens.workflow import InvestigationWorkflow


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--database', default='datalens_demo_v1', type=demo_name)
    actions = parser.add_mutually_exclusive_group()
    actions.add_argument('--setup', action='store_true')
    actions.add_argument('--inject-late-payment', action='store_true')
    actions.add_argument('--repair', action='store_true', help='Manually authorize scoped aggregate reconciliation')
    parser.add_argument('--expect', choices=['baseline','discrepancy','recovered'])
    parser.add_argument('--live-model', action='store_true')
    parser.add_argument('--output', type=Path, default=Path('.flink-state/datalens-demo.json'))
    args = parser.parse_args()
    case = load_cases()[0]
    result = dict(schema_version=1, database=args.database, status='blocked',
                  simulation='Late arrival is reproduced at the database evidence boundary; Kafka/Flink are not exercised.')
    try:
        model = OpenAIModel() if args.live_model else NoModel()
        if args.live_model and not model.config.configured:
            raise ValueError('Model configuration absent')
        params = dict(evidence.connection_params(), dbname=args.database)
        if args.setup:
            setup_demo(args.database, case)
        require_owned(params)
        service = EvidenceService(params)
        request = AssistantRequest(**case['request'])
        before = service.investigate(request)
        result['before'] = before.model_dump(mode='json')
        if args.inject_late_payment:
            inject_late(params, case)
        if args.repair:
            if before.status != 'discrepancy':
                raise ValueError('Repair requires an observed discrepancy')
            repair_demo(params)
        report = InvestigationWorkflow(service, model).run(request)
        result['investigation'] = report.model_dump(mode='json')
        row = report.evidence.comparison.rows[0]
        from decimal import Decimal
        state = ('discrepancy' if report.evidence.status == 'discrepancy' else
                 'recovered' if row.audit_count == row.stored_count == 2 and
                    row.audit_revenue == row.stored_revenue == Decimal('0.30') else 'baseline')
        expected_numbers = {'baseline':(1,1,Decimal('0.10'),Decimal('0.10')),
                            'discrepancy':(2,1,Decimal('0.30'),Decimal('0.10')),
                            'recovered':(2,2,Decimal('0.30'),Decimal('0.30'))}[state]
        result.update(state=state, verified=(row.audit_count,row.stored_count,row.audit_revenue,row.stored_revenue) == expected_numbers)
        result['status'] = 'passed' if result['verified'] and (args.expect is None or state == args.expect) else 'failed'
    except Exception as exc:
        result['status'] = 'blocked' if isinstance(exc, (OperationalError, InsufficientPrivilege, DuplicateDatabase, ValueError)) else 'failed'
        result['error_type'] = type(exc).__name__
        result['reason'] = 'Demo blocked; check PostgreSQL, CREATEDB permission, ownership marker, stage and model configuration locally'
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(result, indent=2) + '\n', encoding='utf-8')
    print(json.dumps({k:v for k,v in result.items() if k not in ('before','investigation')}, indent=2))
    return 0 if result['status'] == 'passed' else 2 if result['status'] == 'blocked' else 1


if __name__ == '__main__':
    sys.exit(main())
