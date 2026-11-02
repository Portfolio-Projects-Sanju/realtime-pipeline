"""Control the PostgreSQL source in the selected development Compose stack."""

import subprocess
import sys
import time

from traffic import COMPOSE, ROOT, positive_float

SERVICE = "postgres-source"


def register_parser(commands):
    parser = commands.add_parser("postgres", help="kontrol PostgreSQL source dev")
    actions = parser.add_subparsers(dest="postgres_command", required=True)
    stop = actions.add_parser("stop", help="hentikan PostgreSQL source")
    stop.add_argument("--duration", type=positive_float, help="detik sebelum PostgreSQL dipulihkan")
    actions.add_parser("start", help="jalankan PostgreSQL source tanpa batas waktu")
    actions.add_parser("resume", help="jalankan kembali PostgreSQL source")
    parser.set_defaults(handler=run)


def compose(*arguments, **kwargs):
    return subprocess.run(COMPOSE + list(arguments), cwd=ROOT, check=True, **kwargs)


def run(args):
    try:
        if args.postgres_command in ("start", "resume"):
            compose("start", SERVICE)
            return 0
        if args.duration is None:
            compose("stop", SERVICE)
            return 0
        result = compose("ps", "--status", "running", "-q", SERVICE,
                         capture_output=True, text=True)
        if not result.stdout.strip():
            raise ValueError("PostgreSQL dev tidak sedang berjalan; eksperimen dibatalkan")
        try:
            compose("stop", SERVICE)
            time.sleep(args.duration)
        finally:
            compose("start", SERVICE)
        return 0
    except KeyboardInterrupt:
        print("Eksperimen dihentikan; pemulihan PostgreSQL telah dicoba.", file=sys.stderr)
        return 130
    except (OSError, ValueError, subprocess.CalledProcessError) as exc:
        print(f"Gagal: {exc}", file=sys.stderr)
        return 1
