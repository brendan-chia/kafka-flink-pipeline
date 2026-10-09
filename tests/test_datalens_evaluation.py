from copy import deepcopy
from decimal import Decimal
import json
import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from datalens.benchmark import (load_cases, audit_rows, run_benchmark, OfflineService,
                                NoModel, evaluate)
from datalens.assistant_models import AssistantRequest, ModelAssessment
from datalens.evaluation_db import demo_name
from datalens.llm import ModelConfig
from datalens.workflow import InvestigationWorkflow


class EvaluationTests(unittest.TestCase):
    def test_fixed_corpus_covers_faults_and_controls(self):
        cases = load_cases()
        self.assertEqual([c['id'] for c in cases], ['late-payment','invalid-payment',
            'processing-interruption','healthy-duplicate-replay','lower-payment-activity','missing-evidence'])
        for case in cases:
            rows, invalid = audit_rows(case)
            self.assertEqual(invalid, case['expected']['invalid_reasons'])
        self.assertEqual(len(audit_rows(cases[3])[0]), 1)
        self.assertEqual(len(cases[3]['deliveries']), 2)
        self.assertLess(Decimal(cases[4]['stored']['revenue']), Decimal(cases[4]['baseline']['revenue']))

    def test_offline_passes_and_never_uses_hosted_credentials(self):
        with patch.dict(os.environ, {'OPENAI_API_KEY':'must-not-send', 'DATALENS_OPENAI_MODEL':'test'}), \
             patch('datalens.llm.OpenAIModel.assess', side_effect=AssertionError('network')):
            report = run_benchmark()
        self.assertEqual(report['status'], 'passed', report)
        self.assertEqual(report['summary']['passed'], 6)
        self.assertEqual(report['summary']['model_calls'], 0)
        self.assertNotIn('must-not-send', json.dumps(report))

    def test_live_without_configuration_is_blocked_not_passed(self):
        with patch.dict(os.environ, {'OPENAI_API_KEY':'','DATALENS_OPENAI_MODEL':''}):
            self.assertEqual(run_benchmark(live=True)['status'], 'blocked')

    def test_numeric_and_citation_corruption_fail_rubric(self):
        case = load_cases()[0]
        service = OfflineService(case)
        report = InvestigationWorkflow(service, NoModel()).run(AssistantRequest(**case['request']))
        report.evidence.comparison.rows[0].audit_revenue = Decimal('0.31')
        scores = evaluate(case, report, service.calls, 0)
        self.assertFalse(scores['checks']['numerical_correctness'])
        self.assertFalse(scores['checks']['evidence_support'])
        report.observations[0].evidence_refs = ['fabricated']
        self.assertFalse(evaluate(case, report, service.calls, 0)['checks']['evidence_support'])

    def test_unjustified_control_hypothesis_rejected(self):
        class Model:
            config = ModelConfig('fake', 'fake')
            def assess(self, payload):
                data = json.loads(payload)
                ref = next(c['ref'] for c in data['citations'] if c['kind']=='runbook')
                return ModelAssessment(hypotheses=[dict(cause='sink_or_recovery', evidence_refs=['comparison',ref])])
        for case in load_cases()[3:]:
            report = InvestigationWorkflow(OfflineService(case), Model()).run(AssistantRequest(**case['request']))
            self.assertEqual(report.model_status, 'invalid_output')
            self.assertEqual(report.suspected_causes, [])

    def test_cost_arithmetic_and_unknown_cost(self):
        case = load_cases()[0]
        service = OfflineService(case)
        report = InvestigationWorkflow(service, NoModel()).run(AssistantRequest(**case['request']))
        result = evaluate(case, report, service.calls, 1, {'input_tokens':1000,'output_tokens':100},
                          (Decimal('2'),Decimal('8')), live=True)
        self.assertEqual(result['estimated_cost_usd'], '0.0028')
        self.assertEqual(evaluate(case, report, service.calls, 1, live=True)['cost_status'], 'unknown')

    def test_mutated_replay_id_is_not_healthy(self):
        case = deepcopy(load_cases()[3])
        case['deliveries'][1]['payload'] = case['deliveries'][1]['payload'].replace('0.30','0.40')
        with self.assertRaises(ValueError): audit_rows(case)

    def test_demo_database_names_cannot_target_existing_project(self):
        for name in ['grabevents','postgres','datalens_demo_x;DROP DATABASE postgres','../demo','datalens_demo_']:
            with self.assertRaises(ValueError): demo_name(name)
        self.assertEqual(demo_name('datalens_demo_v1'), 'datalens_demo_v1')
