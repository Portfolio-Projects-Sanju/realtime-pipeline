import csv
import json
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import capacity


class TestCapacity(unittest.TestCase):
    def test_summary_matches_run_id(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "traffic.csv"
            with path.open("w", newline="") as file:
                writer = csv.DictWriter(file, fieldnames=["run_id", "sukses"])
                writer.writeheader()
                writer.writerows([{"run_id": "first", "sukses": 1}, {"run_id": "second", "sukses": 2}])
            with patch.object(capacity, "CSV", path):
                self.assertEqual(capacity.find_summary("second")["sukses"], "2")
                self.assertIsNone(capacity.find_summary("missing"))

    def test_existing_generator_refuses_to_start(self):
        with tempfile.TemporaryDirectory() as directory, \
             patch.object(capacity, "RUNS", Path(directory)), \
             patch.object(capacity, "command", return_value=SimpleNamespace(stdout="running")), \
             patch.object(capacity.subprocess, "Popen") as popen:
            args = SimpleNamespace(tps=50, duration=10, sample_interval=1, kind="steady",
                                   flash_tps=None, flash_interval=30, flash_duration=5)
            self.assertEqual(capacity.execute(args), 1)
            popen.assert_not_called()
            result = json.loads(next(Path(directory).glob("*/capacity-result.json")).read_text())
            self.assertIn("sudah berjalan", result["error"])

    def test_spike_requires_rate(self):
        with tempfile.TemporaryDirectory() as directory, patch.object(capacity, "RUNS", Path(directory)):
            args = SimpleNamespace(tps=50, duration=10, sample_interval=1, kind="spike",
                                   flash_tps=None, flash_interval=30, flash_duration=5)
            self.assertEqual(capacity.execute(args), 1)


if __name__ == "__main__":
    unittest.main()
