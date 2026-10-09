import importlib.util
from pathlib import Path
import sys
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch

SCRIPTS = Path(__file__).resolve().parents[1] / 'scripts'
sys.path.insert(0, str(SCRIPTS))
spec = importlib.util.spec_from_file_location('recovery_drill', SCRIPTS / 'recovery_drill.py')
drill = importlib.util.module_from_spec(spec)
spec.loader.exec_module(drill)


class RecoveryDrillTests(unittest.TestCase):
    def responses(self):
        return [
            {'jobs': [{'jid': 'a', 'state': 'RUNNING'}]},
            {'interval': 600000}, {'request-id': 'trigger'},
            {'status': {'id': 'COMPLETED'}, 'operation': {'checkpointId': 3}},
            {'latest': {'completed': {'id': 3}}, 'counts': {'in_progress': 0}},
            {'state': 'RUNNING'}, {'latest': {'restored': {'id': 3}}},
        ]

    def test_success_requires_restored_checkpoint_and_duplicate_dlq_evidence(self):
        with tempfile.TemporaryDirectory() as directory:
            args = SimpleNamespace(flink_url='http://localhost:8081', timeout=5,
                                   output=Path(directory) / 'report.json')
            report = {}
            with patch.object(drill, 'request', side_effect=self.responses()), \
                    patch.object(drill.recovery_probe, 'publish'), \
                    patch.object(drill.recovery_probe, 'verify', return_value={'missing_event_ids': []}) as verify, \
                    patch.object(drill.subprocess, 'run') as command:
                drill.run(args, report)
                self.assertEqual(report['status'], 'passed')
                self.assertEqual(report['baseline_checkpoint_id'], 3)
                self.assertEqual(verify.call_args.kwargs, {'min_dlq_deliveries': 2})
                self.assertEqual(command.call_args_list[0].args[0][-4:],
                                 ['kill', '-s', 'SIGKILL', 'taskmanager'])
                self.assertEqual(command.call_args_list[1].args[0][-2:], ['start', 'taskmanager'])

    def test_failed_kill_still_attempts_to_start_taskmanager(self):
        with tempfile.TemporaryDirectory() as directory:
            args = SimpleNamespace(flink_url='http://localhost:8081', timeout=5,
                                   output=Path(directory) / 'report.json')
            with patch.object(drill, 'request', side_effect=self.responses()), \
                    patch.object(drill.recovery_probe, 'publish'), \
                    patch.object(drill.recovery_probe, 'verify', return_value={}), \
                    patch.object(drill.subprocess, 'run', side_effect=[RuntimeError('daemon lost'), None]) as command:
                with self.assertRaisesRegex(RuntimeError, 'daemon lost'):
                    drill.run(args, {})
                self.assertEqual(command.call_args_list[-1].args[0][-2:], ['start', 'taskmanager'])

    def test_select_job_refuses_ambiguous_or_unhealthy_cluster(self):
        for jobs in ([], [{'jid': 'a', 'state': 'RESTARTING'}],
                     [{'jid': 'a', 'state': 'RUNNING'}, {'jid': 'b', 'state': 'RUNNING'}]):
            with self.subTest(jobs=jobs), self.assertRaises(RuntimeError):
                drill.select_job({'jobs': jobs})
        self.assertEqual(drill.select_job({'jobs': [
            {'jid': 'old', 'state': 'CANCELED'}, {'jid': 'a', 'state': 'RUNNING'}]}), 'a')

    def test_new_or_in_progress_checkpoint_invalidates_failure_window(self):
        drill.require_pre_checkpoint({'latest': {'completed': {'id': 3}},
                                      'counts': {'in_progress': 0}}, 3)
        for latest, progress in ((4, 0), (3, 1), (None, 0)):
            with self.subTest(latest=latest, progress=progress), self.assertRaises(RuntimeError):
                drill.require_pre_checkpoint({'latest': {'completed': {'id': latest}},
                                              'counts': {'in_progress': progress}}, 3)

    def test_short_interval_never_publishes_or_injects_failure(self):
        args = SimpleNamespace(flink_url='http://localhost:8081', timeout=5)
        with patch.object(drill, 'request', side_effect=[
                {'jobs': [{'jid': 'a', 'state': 'RUNNING'}]}, {'interval': 30000}]), \
                patch.object(drill.recovery_probe, 'publish') as publish, \
                patch.object(drill.subprocess, 'run') as command:
            with self.assertRaises(RuntimeError):
                drill.run(args, {})
            publish.assert_not_called()
            command.assert_not_called()


if __name__ == '__main__':
    unittest.main()
