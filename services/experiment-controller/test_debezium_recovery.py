import json
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import cli
import debezium_recovery


class TestRecoveryExperiment(unittest.TestCase):
    def setUp(self):
        temp = tempfile.TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        self.runs = Path(temp.name)
        patcher = patch.object(debezium_recovery, "RUNS", self.runs)
        patcher.start()
        self.addCleanup(patcher.stop)

    def test_lsn_comparison(self):
        self.assertGreater(debezium_recovery.lsn_number("1/0"), debezium_recovery.lsn_number("0/FFFFFFFF"))

    @patch("debezium_recovery.CONFIG")
    @patch("debezium_recovery.time.sleep")
    @patch("debezium_recovery.status")
    @patch("debezium_recovery.wal")
    @patch("debezium_recovery.subprocess.run")
    @patch("debezium_recovery.compose")
    def test_fault_and_recovery_recorded(self, compose, inspect, wal, status, sleep, config):
        def command(*args, **kwargs):
            if args[:3] == ("ps", "--status", "running"):
                return SimpleNamespace(stdout="" if args[-1] == "debezium" and compose.stop_called else "id\n")
            if args[0] == "stop":
                compose.stop_called = True
            return SimpleNamespace(stdout="")
        compose.stop_called = False
        compose.side_effect = command
        inspect.side_effect = [SimpleNamespace(stdout="realtime-pipeline-dev:debezium"),
                               SimpleNamespace(stdout="realtime-pipeline-dev:postgres-source")]
        status.return_value = {"connector": {"state": "RUNNING"}, "tasks": [{"state": "RUNNING"}]}
        wal.return_value = {"current_lsn": "0/10", "confirmed_flush_lsn": "0/10", "backlog_bytes": 0}
        config.read_text.return_value = "request: {}"
        args = SimpleNamespace(duration=.001, baseline_duration=.001, recovery_timeout=1,
                               sample_interval=.001, connector="connector", slot="slot")
        self.assertEqual(debezium_recovery.run_debezium_recovery(args), 0)
        result = json.loads(next(self.runs.glob('*/debezium-recovery-result.json')).read_text())
        self.assertTrue(result["fault_verified"])
        self.assertTrue(result["service_start_succeeded"])
        self.assertEqual(result["status"], "completed")
        self.assertIn("comparison", result)
        self.assertTrue(any(c.args == ("start", "debezium") for c in compose.call_args_list))

    @patch("debezium_recovery.compose")
    def test_preflight_failure_never_stops(self, compose):
        compose.return_value = SimpleNamespace(stdout="")
        args = SimpleNamespace(duration=1, baseline_duration=1, recovery_timeout=1,
                               sample_interval=1, connector="connector", slot="slot")
        self.assertEqual(debezium_recovery.run_debezium_recovery(args), 1)
        self.assertFalse(any(c.args and c.args[0] == "stop" for c in compose.call_args_list))

    def test_invalid_slot_rejected(self):
        with self.assertRaises(SystemExit):
            cli.main(["--environment", "dev", "experiment", "debezium-recovery",
                      "--duration", "1", "--slot", "bad'quote"])


if __name__ == "__main__":
    unittest.main()
