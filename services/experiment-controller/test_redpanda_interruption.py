import json
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import redpanda_interruption as scenario


class TestRedpandaInterruption(unittest.TestCase):
    def setUp(self):
        temp = tempfile.TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        self.runs = Path(temp.name)
        patcher = patch.object(scenario, "RUNS", self.runs)
        patcher.start()
        self.addCleanup(patcher.stop)
        self.args = SimpleNamespace(duration=.001, baseline_duration=.001, recovery_timeout=1,
                                    sample_interval=.001, connector="connector", slot="slot", group="group")
        self.sample = {"timestamp": "now", "errors": {},
                       "connector": {"connector": {"state": "RUNNING"}, "tasks": [{"state": "RUNNING"}]},
                       "consumer": {"running": True, "health": "healthy"},
                       "group": {"state": "Stable", "total_lag": 0, "partitions": [{}]},
                       "wal": {"current_lsn": "0/10", "confirmed_flush_lsn": "0/10"}}

    @patch("redpanda_interruption.CONFIG")
    @patch("redpanda_interruption.time.sleep")
    @patch("redpanda_interruption.observe")
    @patch("redpanda_interruption.inspect_service")
    @patch("redpanda_interruption.compose")
    def test_success_verifies_recovery(self, compose, inspect, observe, sleep, config):
        inspect.return_value = {"health": "healthy"}
        observe.side_effect = lambda args, phase: {**self.sample, "phase": phase}
        compose.return_value = SimpleNamespace(stdout="")
        config.read_text.return_value = "request: {}"
        self.assertEqual(scenario.execute(self.args), 0)
        result = json.loads(next(self.runs.glob('*/redpanda-interruption-result.json')).read_text())
        self.assertTrue(result["fault_verified"])
        self.assertTrue(result["service_start_succeeded"])
        self.assertEqual(result["status"], "completed")
        self.assertTrue(any(c.args == ("start", "redpanda") for c in compose.call_args_list))

    @patch("redpanda_interruption.CONFIG")
    @patch("redpanda_interruption.time.sleep")
    @patch("redpanda_interruption.observe")
    @patch("redpanda_interruption.inspect_service")
    @patch("redpanda_interruption.compose")
    def test_fault_failure_restores(self, compose, inspect, observe, sleep, config):
        inspect.return_value = {"health": "healthy"}
        observe.side_effect = lambda args, phase: {**self.sample, "phase": phase}
        config.read_text.return_value = "request: {}"
        def command(*args, **kwargs):
            if args[0] == "stop":
                raise RuntimeError("stop failed")
            return SimpleNamespace(stdout="")
        compose.side_effect = command
        self.assertEqual(scenario.execute(self.args), 1)
        self.assertTrue(any(c.args == ("start", "redpanda") for c in compose.call_args_list))

    @patch("redpanda_interruption.compose")
    @patch("redpanda_interruption.inspect_service")
    def test_preflight_failure_does_not_stop(self, inspect, compose):
        inspect.side_effect = RuntimeError("wrong project")
        self.assertEqual(scenario.execute(self.args), 1)
        compose.assert_not_called()

    def test_ready_requires_stable_group_and_healthy_services(self):
        self.assertTrue(scenario.ready(self.sample, {"health": "healthy"}))
        self.assertFalse(scenario.ready(self.sample, {"health": "unhealthy"}))
        self.assertFalse(scenario.ready({**self.sample, "group": None}, {"health": "healthy"}))


if __name__ == "__main__":
    unittest.main()
