import json
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import clickhouse_recovery as scenario


class TestClickHouseRecovery(unittest.TestCase):
    def setUp(self):
        temp = tempfile.TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        patcher = patch.object(scenario, "RUNS", Path(temp.name))
        patcher.start()
        self.addCleanup(patcher.stop)
        self.runs = Path(temp.name)
        self.args = SimpleNamespace(duration=.001, baseline_duration=.001, recovery_timeout=1,
                                    sample_interval=.001, connector="connector", group="group")

    @patch("clickhouse_recovery.CONFIG")
    @patch("clickhouse_recovery.reconcile")
    @patch("clickhouse_recovery.source_orders", return_value=[2])
    @patch("clickhouse_recovery.source_max", return_value=1)
    @patch("clickhouse_recovery.time.sleep")
    @patch("clickhouse_recovery.status")
    @patch("clickhouse_recovery.group_status")
    @patch("clickhouse_recovery.inspect_service")
    @patch("clickhouse_recovery.compose")
    def test_interruption_and_drain(self, compose, inspect, group, status, sleep, source_max, source_orders, reconcile, config):
        inspect.return_value = {"health": "healthy", "restart_count": 0}
        status.return_value = {"connector": {"state": "RUNNING"}, "tasks": [{"state": "RUNNING"}]}
        counter = iter(range(100000))
        def sample(_):
            n = next(counter)
            return {"state": "Stable", "partitions": [{"topic": "orders", "partition": 0,
                    "log_end_offset": n + 1, "current_offset": n + 1}]}
        group.side_effect = sample
        compose.return_value = SimpleNamespace(stdout="")
        config.read_text.return_value = "request: {}"
        reconcile.return_value = {"status": "complete", "missing_order_ids": [], "duplicate_create_counts": {}}
        self.assertEqual(scenario.execute(self.args), 0)
        result = json.loads(next(self.runs.glob('*/clickhouse-recovery-result.json')).read_text())
        self.assertTrue(result["fault_verified"])
        self.assertTrue(result["service_start_succeeded"])
        self.assertGreater(result["backlog_growth_offsets"], 0)
        self.assertEqual(result["reconciliation"]["status"], "complete")
        self.assertTrue(any(c.args == ("start", "clickhouse") for c in compose.call_args_list))

    @patch("clickhouse_recovery.CONFIG")
    @patch("clickhouse_recovery.source_max", return_value=1)
    @patch("clickhouse_recovery.time.sleep")
    @patch("clickhouse_recovery.status")
    @patch("clickhouse_recovery.group_status")
    @patch("clickhouse_recovery.inspect_service")
    @patch("clickhouse_recovery.compose")
    def test_stop_failure_still_attempts_restore(self, compose, inspect, group, status, sleep, source_max, config):
        inspect.return_value = {"health": "healthy", "restart_count": 0}
        status.return_value = {"connector": {"state": "RUNNING"}, "tasks": [{"state": "RUNNING"}]}
        counter = iter(range(100000))
        group.side_effect = lambda _: {"state": "Stable", "partitions": [{"topic": "orders", "partition": 0, "log_end_offset": next(counter)}]}
        config.read_text.return_value = "request: {}"
        def command(*args, **kwargs):
            if args[0] == "stop":
                raise RuntimeError("stop failed")
            return SimpleNamespace(stdout="")
        compose.side_effect = command
        self.assertEqual(scenario.execute(self.args), 1)
        self.assertTrue(any(c.args == ("start", "clickhouse") for c in compose.call_args_list))

    @patch("clickhouse_recovery.compose")
    @patch("clickhouse_recovery.inspect_service", side_effect=RuntimeError("wrong project"))
    def test_preflight_failure_no_stop(self, inspect, compose):
        self.assertEqual(scenario.execute(self.args), 1)
        compose.assert_not_called()


if __name__ == "__main__":
    unittest.main()
