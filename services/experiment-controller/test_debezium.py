import subprocess
import unittest
from types import SimpleNamespace
from unittest.mock import patch

import cli
import debezium


class TestDebeziumCLI(unittest.TestCase):
    @patch("debezium.subprocess.run")
    def test_service_commands(self, run):
        for action, expected in (("start", ["start"]), ("resume", ["start"]),
                                 ("restart", ["restart"]), ("stop", ["stop"]),
                                 ("status", ["ps", "--all"])):
            self.assertEqual(cli.main(["--environment", "dev", "debezium", action]), 0)
            self.assertEqual(run.call_args.args[0], debezium.COMPOSE + expected + ["debezium"])

    @patch("debezium.time.sleep")
    @patch("debezium.subprocess.run")
    def test_timed_stop_restores(self, run, sleep):
        run.return_value = SimpleNamespace(stdout="id\n")
        self.assertEqual(cli.main(["--environment", "dev", "debezium", "stop", "--duration", "5"]), 0)
        self.assertEqual([c.args[0][-2:] for c in run.call_args_list[1:]],
                         [["stop", "debezium"], ["start", "debezium"]])
        sleep.assert_called_once_with(5)

    @patch("debezium.time.sleep", side_effect=KeyboardInterrupt)
    @patch("debezium.subprocess.run")
    def test_interrupt_restores(self, run, sleep):
        run.return_value = SimpleNamespace(stdout="id")
        self.assertEqual(cli.main(["--environment", "dev", "debezium", "stop", "--duration", "5"]), 130)
        self.assertEqual(run.call_args.args[0][-2:], ["start", "debezium"])

    @patch("debezium.subprocess.run")
    def test_stopped_service_is_not_started(self, run):
        run.return_value = SimpleNamespace(stdout="")
        self.assertEqual(cli.main(["--environment", "dev", "debezium", "stop", "--duration", "5"]), 1)
        self.assertEqual(run.call_count, 1)

    @patch("debezium.subprocess.run")
    def test_stop_failure_attempts_recovery(self, run):
        run.side_effect = [SimpleNamespace(stdout="id"), subprocess.CalledProcessError(1, "stop"), None]
        self.assertEqual(cli.main(["--environment", "dev", "debezium", "stop", "--duration", "5"]), 1)
        self.assertEqual(run.call_args.args[0][-2:], ["start", "debezium"])

    def test_invalid_duration_or_environment(self):
        for args in (["--environment", "dev", "debezium", "stop", "--duration", "0"],
                     ["--environment", "production", "debezium", "stop"]):
            with self.assertRaises(SystemExit):
                cli.main(args)


if __name__ == "__main__":
    unittest.main()
