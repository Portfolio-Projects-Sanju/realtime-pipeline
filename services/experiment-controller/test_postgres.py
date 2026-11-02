import subprocess
import unittest
from types import SimpleNamespace
from unittest.mock import call, patch

import cli
import postgres


class TestPostgresCLI(unittest.TestCase):
    @patch("postgres.subprocess.run")
    def test_start_resume_stop_without_duration(self, run):
        for action, expected in (("start", "start"), ("resume", "start"), ("stop", "stop")):
            self.assertEqual(cli.main(["--environment", "dev", "postgres", action]), 0)
            self.assertEqual(run.call_args.args[0], postgres.COMPOSE + [expected, postgres.SERVICE])

    @patch("postgres.time.sleep")
    @patch("postgres.subprocess.run")
    def test_timed_stop_restores(self, run, sleep):
        run.return_value = SimpleNamespace(stdout="container-id\n")
        self.assertEqual(cli.main(["--environment", "dev", "postgres", "stop", "--duration", "5"]), 0)
        self.assertEqual([c.args[0][-2:] for c in run.call_args_list[1:]],
                         [["stop", postgres.SERVICE], ["start", postgres.SERVICE]])
        sleep.assert_called_once_with(5)

    @patch("postgres.time.sleep", side_effect=KeyboardInterrupt)
    @patch("postgres.subprocess.run")
    def test_interrupt_restores(self, run, sleep):
        run.return_value = SimpleNamespace(stdout="container-id\n")
        self.assertEqual(cli.main(["--environment", "dev", "postgres", "stop", "--duration", "5"]), 130)
        self.assertEqual(run.call_args.args[0][-2:], ["start", postgres.SERVICE])

    @patch("postgres.subprocess.run")
    def test_stopped_service_is_not_started(self, run):
        run.return_value = SimpleNamespace(stdout="")
        self.assertEqual(cli.main(["--environment", "dev", "postgres", "stop", "--duration", "5"]), 1)
        self.assertEqual(run.call_count, 1)

    @patch("postgres.subprocess.run")
    def test_stop_failure_still_attempts_recovery(self, run):
        run.side_effect = [SimpleNamespace(stdout="id"), subprocess.CalledProcessError(1, "stop"), None]
        self.assertEqual(cli.main(["--environment", "dev", "postgres", "stop", "--duration", "5"]), 1)
        self.assertEqual(run.call_args.args[0][-2:], ["start", postgres.SERVICE])


if __name__ == "__main__":
    unittest.main()
