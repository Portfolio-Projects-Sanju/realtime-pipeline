import json
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import reconcile_orders as r


class TestReconcile(unittest.TestCase):
    def test_timestamp_requires_timezone(self):
        with self.assertRaises(ValueError):
            r.utc("2026-01-01T00:00:00")

    @patch("reconcile_orders.reconcile")
    @patch("reconcile_orders.source_ids", return_value=[1, 2])
    @patch("reconcile_orders.inspect_service")
    @patch("reconcile_orders.compose")
    def test_records_missing_and_fails(self, compose, inspect, source_ids, reconcile):
        compose.return_value = SimpleNamespace(stdout="UTC\n")
        reconcile.return_value = {"status": "complete", "missing_order_ids": [2], "duplicate_create_counts": {}}
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "run.json"
            path.write_text(json.dumps({"run_id": "test", "environment": "dev", "status": "completed", "fault_started_at": "2026-01-01T00:00:00+00:00", "recovery_started_at": "2026-01-01T00:01:00+00:00"}))
            args = SimpleNamespace(run=path, wait=.001, sample_interval=.001)
            self.assertEqual(r.run(args), 1)
            result = json.loads((path.parent / "orders-reconciliation.json").read_text())
            self.assertEqual(result["reconciliation"]["missing_order_ids"], [2])

    @patch("reconcile_orders.inspect_service")
    def test_rejects_production(self, inspect):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "run.json"
            path.write_text(json.dumps({"environment": "production", "status": "completed"}))
            self.assertEqual(r.run(SimpleNamespace(run=path, wait=1, sample_interval=1)), 1)
            inspect.assert_not_called()


if __name__ == "__main__":
    unittest.main()
