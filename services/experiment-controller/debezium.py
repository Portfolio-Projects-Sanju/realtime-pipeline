"""Control the Debezium Connect service on the development stack."""

import subprocess
import sys
import time

from traffic import COMPOSE, ROOT, positive_float

SERVICE = "debezium"


def register_parser(commands):
    parser = commands.add_parser("debezium", help="kontrol service Debezium dev")
    actions = parser.add_subparsers(dest="debezium_command", required=True)
    stop = actions.add_parser("stop", help="hentikan service Debezium")
    stop.add_argument("--duration", type=positive_float,
                      help="detik sebelum service dipulihkan")
    actions.add_parser("start", help="jalankan container Debezium yang sudah ada")
    actions.add_parser("resume", help="jalankan kembali container Debezium")
    actions.add_parser("restart", help="restart container Debezium")
    actions.add_parser("status", help="tampilkan status container Debezium")
    parser.set_defaults(handler=run)


def compose(*arguments, **kwargs):
    return subprocess.run(COMPOSE + list(arguments), cwd=ROOT, check=True, **kwargs)


def run(args):
    try:
        action = args.debezium_command
        if action == "status":
            compose("ps", "--all", SERVICE)
        elif action in ("start", "resume", "restart"):
            compose("restart" if action == "restart" else "start", SERVICE)
        elif args.duration is None:
            compose("stop", SERVICE)
        else:
            result = compose("ps", "--status", "running", "-q", SERVICE,
                             capture_output=True, text=True)
            if not result.stdout.strip():
                raise ValueError("Debezium dev tidak sedang berjalan; eksperimen dibatalkan")
            try:
                compose("stop", SERVICE)
                time.sleep(args.duration)
            finally:
                compose("start", SERVICE)
        return 0
    except KeyboardInterrupt:
        print("Eksperimen dihentikan; pemulihan Debezium telah dicoba.", file=sys.stderr)
        return 130
    except (OSError, ValueError, subprocess.CalledProcessError) as exc:
        print(f"Gagal: {exc}", file=sys.stderr)
        return 1
