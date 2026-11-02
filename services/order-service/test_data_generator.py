import csv
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import data_generator


CONFIG = {"request": {"base_rate_per_second": 5, "max_concurrency": 50, "timeout_seconds": 5},
          "flash_sale": {"enabled": True, "rate_per_second": 400, "interval_seconds": 30, "duration_seconds": 2},
          "experiment": {"duration_seconds": 30}}


class TestDataGenerator(unittest.TestCase):
    @patch("data_generator.SessionLocal")
    def test_seed_skips_existing_data(self, session_factory):
        db = session_factory.return_value
        db.query.return_value.count.return_value = 101

        data_generator.seed_db()

        db.add_all.assert_not_called()
        db.commit.assert_not_called()
        db.close.assert_called_once()

    @patch("data_generator.requests.post")
    def test_request_outcomes_and_latency(self, post):
        stats = data_generator.TrafficStats()
        response = unittest.mock.Mock(status_code=201)
        error_response = unittest.mock.Mock(status_code=503)
        post.side_effect = [response, error_response,
                            data_generator.requests.exceptions.ConnectionError(),
                            data_generator.requests.exceptions.Timeout()]
        for _ in range(4):
            data_generator.send_transaction(1, 5, stats)
        self.assertEqual((stats.attempted, stats.success, stats.failed, stats.timeout),
                         (4, 1, 2, 1))
        self.assertGreaterEqual(stats.latency_total, 0)
        self.assertIsNotNone(stats.latency_min)
        with tempfile.TemporaryDirectory() as directory:
            path = stats.write_csv(Path(directory) / "logs", CONFIG)
            with path.open(newline="", encoding="utf-8") as file:
                rows = list(csv.DictReader(file))
            self.assertEqual(len(rows), 1)
            self.assertEqual([rows[0][key] for key in ("attempted", "sukses", "gagal", "timeout")],
                             ["4", "1", "2", "1"])
            self.assertTrue(rows[0]["timestamp"].endswith("+00:00"))
            self.assertEqual(rows[0]["base_rate_per_second"], "5")
            self.assertEqual(rows[0]["duration_seconds"], "30")
            self.assertEqual(rows[0]["flash_sale_rate_per_second"], "400")
            self.assertGreaterEqual(float(rows[0]["latency"]), 0)
            self.assertEqual(path, stats.write_csv(Path(directory) / "logs", CONFIG))
            with path.open(newline="", encoding="utf-8") as file:
                self.assertEqual(len(list(csv.DictReader(file))), 2)

    def test_migrates_old_csv(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "traffic.csv"
            path.write_text("timestamp,attempted,sukses,gagal,timeout,latency\nold,1,1,0,0,5\n")
            data_generator.TrafficStats().write_csv(directory, CONFIG)
            with path.open(newline="", encoding="utf-8") as file:
                rows = list(csv.DictReader(file))
            self.assertEqual(len(rows), 2)
            self.assertEqual(rows[0]["timestamp"], "old")
            self.assertEqual(rows[0]["base_rate_per_second"], "")
            self.assertEqual(rows[1]["duration_seconds"], "30")

    @patch.dict("data_generator.os.environ", {"TRAFFIC_LOG_DIR": "/tmp/traffic-logs"})
    @patch("data_generator.TrafficStats.write_csv")
    @patch("data_generator.__file__", "/app/data_generator.py")
    def test_container_log_directory(self, write_csv):
        config = {"request": {"base_rate_per_second": 1, "max_concurrency": 1,
                              "timeout_seconds": 1},
                  "flash_sale": {"enabled": False},
                  "experiment": {"duration_seconds": 0}}
        data_generator.generate_traffic(config)
        write_csv.assert_called_once_with(Path("/tmp/traffic-logs"), config)

    def test_percentiles_and_legacy_csv_migration(self):
        stats = data_generator.TrafficStats()
        stats.start_monotonic -= 1  # Ensure a measurable duration even on fast CI runners.
        for latency in (0.01, 0.02, 0.03, 0.04):
            stats.record("success", latency)
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "traffic.csv"
            path.write_text("timestamp,attempted,sukses,gagal,timeout,latency,base_rate_per_second,duration_seconds,flash_sale_enabled,flash_sale_rate_per_second,flash_sale_interval_seconds,flash_sale_duration_seconds,max_concurrency,timeout_seconds\nold,1,1,0,0,5,5,30,False,100,30,2,50,5\n")
            stats.write_csv(directory, CONFIG)
            with path.open(newline="", encoding="utf-8") as file:
                rows = list(csv.DictReader(file))
            self.assertEqual(rows[0]["latency_p95_ms"], "")
            self.assertEqual(float(rows[1]["latency_p50_ms"]), 20)
            self.assertEqual(float(rows[1]["latency_p95_ms"]), 40)
            self.assertEqual(float(rows[1]["latency_p99_ms"]), 40)
            self.assertGreater(float(rows[1]["actual_duration_seconds"]), 0)

    def test_empty_summary(self):
        with tempfile.TemporaryDirectory() as directory:
            path = data_generator.TrafficStats().write_csv(directory, CONFIG)
            with path.open(newline="", encoding="utf-8") as file:
                row = next(csv.DictReader(file))
            self.assertEqual(row["attempted"], "0")
            self.assertEqual(float(row["latency"]), 0)


if __name__ == "__main__":
    unittest.main()
