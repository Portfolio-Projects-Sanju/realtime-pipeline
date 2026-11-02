import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import yaml
import cli
import traffic


class TestTrafficCLI(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.config = Path(self.temp.name) / "traffic.yaml"
        self.original = {"request": {"base_rate_per_second": 5, "max_concurrency": 50, "timeout_seconds": 5},
                         "flash_sale": {"enabled": False, "rate_per_second": 100,
                                        "interval_seconds": 30, "duration_seconds": 2},
                         "experiment": {"duration_seconds": 30}}
        self.config.write_text(yaml.safe_dump(self.original))
        patcher = patch.object(traffic, "CONFIG", self.config)
        patcher.start()
        self.addCleanup(patcher.stop)

    @patch("traffic.subprocess.run")
    def test_commands_target_only_dev_generator(self, run):
        for action, expected in (("start", ["up", "-d", "--no-deps"]),
                                 ("stop", ["stop"]), ("restart", ["restart"])):
            self.assertEqual(cli.main(["--environment", "dev", "traffic", action]), 0)
            self.assertEqual(run.call_args.args[0], traffic.COMPOSE + expected + ["traffic-generator"])

    @patch("traffic.subprocess.run")
    def test_start_with_config_applies_before_recreate(self, run):
        result = cli.main(["--environment", "dev", "traffic", "start", "config",
                           "--tps", "20", "--duration", "60"])
        self.assertEqual(result, 0)
        config = yaml.safe_load(self.config.read_text())
        self.assertEqual(config["request"]["base_rate_per_second"], 20)
        self.assertEqual(config["experiment"]["duration_seconds"], 60)
        self.assertEqual(run.call_args.args[0][-5:],
                         ["up", "-d", "--no-deps", "--force-recreate", "traffic-generator"])

    @patch("traffic.subprocess.run")
    def test_restart_with_invalid_config_does_not_run(self, run):
        self.assertEqual(cli.main(["--environment", "dev", "traffic", "restart",
                                   "--flash-duration", "31"]), 1)
        run.assert_not_called()

    def test_update_config_and_unlimited(self):
        self.assertEqual(cli.main(["--environment", "dev", "traffic", "config", "--tps", "12", "--duration", "60",
                                   "--flash-sale", "--flash-tps", "50"]), 0)
        config = yaml.safe_load(self.config.read_text())
        self.assertEqual(config["request"]["base_rate_per_second"], 12)
        self.assertEqual(config["experiment"]["duration_seconds"], 60)
        self.assertTrue(config["flash_sale"]["enabled"])
        self.assertEqual(config["flash_sale"]["rate_per_second"], 50)
        self.assertEqual(cli.main(["--environment", "dev", "traffic", "config", "--unlimited", "--no-flash-sale"]), 0)
        config = yaml.safe_load(self.config.read_text())
        self.assertIsNone(config["experiment"]["duration_seconds"])
        self.assertFalse(config["flash_sale"]["enabled"])

    def test_invalid_settings_do_not_write(self):
        self.assertEqual(cli.main(["--environment", "dev", "traffic", "config", "--flash-duration", "31"]), 1)
        self.assertEqual(yaml.safe_load(self.config.read_text()), self.original)
        with self.assertRaises(SystemExit):
            cli.main(["--environment", "dev", "traffic", "config", "--tps", "0"])
        with self.assertRaises(SystemExit):
            cli.main(["--environment", "production", "traffic", "stop"])


if __name__ == "__main__":
    unittest.main()
