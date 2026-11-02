import subprocess
import unittest
from types import SimpleNamespace
from unittest.mock import patch

import cli
import consumer


class TestConsumerCLI(unittest.TestCase):
    @patch("consumer.subprocess.run")
    def test_service_commands(self, run):
        for action, expected in (("start", ["start"]), ("resume", ["start"]),
                                 ("restart", ["restart"]), ("stop", ["stop"]),
                                 ("status", ["ps", "--all"])):
            self.assertEqual(cli.main(["--environment", "dev", "consumer", action]), 0)
            self.assertEqual(run.call_args.args[0], consumer.COMPOSE + expected + ["consumer"])

    @patch("consumer.time.sleep")
    @patch("consumer.subprocess.run")
    def test_timed_stop_restores(self, run, sleep):
        run.return_value = SimpleNamespace(stdout="id\n")
        self.assertEqual(cli.main(["--environment", "dev", "consumer", "stop", "--duration", "5"]), 0)
        self.assertEqual([c.args[0][-2:] for c in run.call_args_list[1:]],
                         [["stop", "consumer"], ["start", "consumer"]])
        sleep.assert_called_once_with(5)

    @patch("consumer.time.sleep", side_effect=KeyboardInterrupt)
    @patch("consumer.subprocess.run")
    def test_interrupt_restores(self, run, sleep):
        run.return_value = SimpleNamespace(stdout="id")
        self.assertEqual(cli.main(["--environment", "dev", "consumer", "stop", "--duration", "5"]), 130)
        self.assertEqual(run.call_args.args[0][-2:], ["start", "consumer"])

    @patch("consumer.subprocess.run")
    def test_stopped_service_is_not_started(self, run):
        run.return_value = SimpleNamespace(stdout="")
        self.assertEqual(cli.main(["--environment", "dev", "consumer", "stop", "--duration", "5"]), 1)
        self.assertEqual(run.call_count, 1)

    @patch("consumer.subprocess.run")
    def test_stop_failure_attempts_recovery(self, run):
        run.side_effect = [SimpleNamespace(stdout="id"), subprocess.CalledProcessError(1, "stop"), None]
        self.assertEqual(cli.main(["--environment", "dev", "consumer", "stop", "--duration", "5"]), 1)
        self.assertEqual(run.call_args.args[0][-2:], ["start", "consumer"])

    def test_invalid_duration_or_environment(self):
        for args in (["--environment", "dev", "consumer", "stop", "--duration", "0"],
                     ["--environment", "production", "consumer", "stop"]):
            with self.assertRaises(SystemExit):
                cli.main(args)


if __name__ == "__main__":
    unittest.main()
