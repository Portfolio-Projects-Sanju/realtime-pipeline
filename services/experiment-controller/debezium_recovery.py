"""Debezium recovery experiment using PostgreSQL WAL positions."""
import argparse
import fcntl
import json
import os
from pathlib import Path
import re
import signal
import subprocess
import sys
import time
import urllib.error
import urllib.request
import uuid
from datetime import datetime, timezone

from traffic import COMPOSE, ROOT, CONFIG, positive_float

RUNS = Path(__file__).resolve().parent / "logs" / "debezium-recovery" / "runs"


def timestamp():
    return datetime.now(timezone.utc).isoformat()


def compose(*args, **kwargs):
    return subprocess.run(COMPOSE + list(args), cwd=ROOT, check=True, text=True,
                          timeout=30, **kwargs)


def status(connector):
    port = compose("port", "debezium", "8083", capture_output=True).stdout.strip()
    match = re.search(r":(\d+)$", port)
    if not match:
        raise RuntimeError("port Debezium dev tidak ditemukan")
    url = f"http://127.0.0.1:{match.group(1)}/connectors/{connector}/status"
    with urllib.request.urlopen(url, timeout=5) as response:
        return json.load(response)


def healthy(value):
    return (value.get("connector", {}).get("state") == "RUNNING" and
            bool(value.get("tasks")) and
            all(task.get("state") == "RUNNING" for task in value["tasks"]))


def wal(slot):
    # Identifiers are restricted at CLI parsing; no user value is passed to a shell.
    sql = ("SELECT pg_current_wal_lsn()::text, confirmed_flush_lsn::text, "
           "pg_wal_lsn_diff(pg_current_wal_lsn(), confirmed_flush_lsn)::bigint "
           "FROM pg_replication_slots WHERE slot_name = '" + slot + "' AND slot_type = 'logical'")
    result = compose("exec", "-T", "postgres-source", "sh", "-c",
                     'exec psql -X -v ON_ERROR_STOP=1 -U "$POSTGRES_USER" -d "$POSTGRES_DB" -At -F "|" -c "$1"',
                     "sh", sql, capture_output=True).stdout.strip().split("|")
    if len(result) != 3 or not result[1] or not result[2]:
        raise RuntimeError("replication slot tidak ditemukan atau belum memiliki confirmed_flush_lsn")
    return {"current_lsn": result[0], "confirmed_flush_lsn": result[1], "backlog_bytes": int(result[2])}


def lsn_number(value):
    high, low = value.split("/")
    return (int(high, 16) << 32) + int(low, 16)


def name(value):
    if not re.fullmatch(r"[A-Za-z0-9_.-]+", value):
        raise argparse.ArgumentTypeError("gunakan hanya huruf, angka, titik, garis bawah, atau tanda hubung")
    return value


def register_parser(actions):
    run = actions.add_parser("debezium-recovery", help="baseline, stop, dan pemulihan Debezium")
    run.add_argument("--duration", type=positive_float, required=True, help="lama gangguan dalam detik")
    run.add_argument("--baseline-duration", type=positive_float, default=30)
    run.add_argument("--recovery-timeout", type=positive_float, default=300)
    run.add_argument("--sample-interval", type=positive_float, default=5)
    run.add_argument("--connector", type=name, default="ecommerce-postgres-source")
    run.add_argument("--slot", type=name, default="ecommerce_debezium_slot")
    run.set_defaults(handler=run_debezium_recovery)


def save(path, result):
    temporary = path.with_suffix(".tmp")
    temporary.write_text(json.dumps(result, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    os.replace(temporary, path)


def run_debezium_recovery(args):
    RUNS.mkdir(parents=True, exist_ok=True)
    with (RUNS / ".debezium-recovery.lock").open("a+") as lock:
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            print("Eksperimen lain sedang berjalan.", file=sys.stderr)
            return 1
        previous = signal.getsignal(signal.SIGTERM)
        def stop_on_sigterm(signum, frame):
            raise KeyboardInterrupt
        signal.signal(signal.SIGTERM, stop_on_sigterm)
        try:
            return execute(args)
        finally:
            signal.signal(signal.SIGTERM, previous)


def execute(args):
    run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ") + "-" + uuid.uuid4().hex[:8]
    path = RUNS / run_id / "debezium-recovery-result.json"
    path.parent.mkdir(parents=True)
    result = {"run_id": run_id, "environment": "dev", "compose_project": "realtime-pipeline-dev",
              "mechanism": "stop/start Compose service debezium", "started_at": timestamp(),
              "parameters": {"duration_seconds": args.duration, "baseline_duration_seconds": args.baseline_duration,
                             "recovery_timeout_seconds": args.recovery_timeout, "sample_interval_seconds": args.sample_interval,
                             "connector": args.connector, "slot": args.slot},
              "limitations": ["WAL backlog diukur dalam byte; bukan CDC delay dalam detik atau latency end-to-end.",
                              "Baseline adalah observasi sebelum gangguan; kestabilan workload harus diverifikasi operator.",
                              "Recovery WAL tidak membuktikan data sudah tiba di ClickHouse."],
              "samples": [], "status": "preflight", "recovery_attempted": False}
    fault_started = False
    failed = False
    interrupted = False
    def record(phase, connector_status=None):
        sample = {"timestamp": timestamp(), "phase": phase, **wal(args.slot)}
        if connector_status is not None:
            sample["connector_status"] = connector_status
        result["samples"].append(sample)
        save(path, result)
        return sample
    try:
        save(path, result)
        # Reject ambiguous projects and require the existing explicitly named dev containers.
        for service in ("debezium", "postgres-source"):
            ids = compose("ps", "--status", "running", "-q", service, capture_output=True).stdout.split()
            if len(ids) != 1:
                raise RuntimeError(f"service {service} dev harus memiliki tepat satu container berjalan")
            labels = subprocess.run(["docker", "inspect", "--format", '{{ index .Config.Labels "com.docker.compose.project" }}:{{ index .Config.Labels "com.docker.compose.service" }}', ids[0]],
                                    text=True, capture_output=True, check=True, timeout=30).stdout.strip()
            if labels != f"realtime-pipeline-dev:{service}":
                raise RuntimeError(f"label container {service} tidak sesuai stack dev")
        state = status(args.connector)
        if not healthy(state):
            raise RuntimeError("connector atau task belum RUNNING")
        result["traffic_config"] = CONFIG.read_text(encoding="utf-8")
        result["status"] = "baseline"
        record("baseline", state)
        end = time.monotonic() + args.baseline_duration
        while time.monotonic() < end:
            time.sleep(min(args.sample_interval, max(0, end - time.monotonic())))
            record("baseline")
        result["status"] = "fault"
        result["fault_started_at"] = timestamp()
        save(path, result)
        fault_started = True
        compose("stop", "debezium")
        still_running = compose("ps", "--status", "running", "-q", "debezium", capture_output=True)
        if still_running.stdout.strip():
            raise RuntimeError("Debezium masih berjalan setelah perintah stop")
        result["fault_verified"] = True
        save(path, result)
        end = time.monotonic() + args.duration
        while time.monotonic() < end:
            record("fault")
            time.sleep(min(args.sample_interval, max(0, end - time.monotonic())))
        target = record("fault_end")
        result["catchup_target_lsn"] = target["current_lsn"]
    except KeyboardInterrupt:
        interrupted = failed = True
        result["error"] = "interrupted"
    except Exception as exc:
        failed = True
        result["error"] = str(exc)
    finally:
        if fault_started:
            result["recovery_attempted"] = True
            result["recovery_started_at"] = timestamp()
            try:
                compose("start", "debezium")
                result["service_start_succeeded"] = True
            except (OSError, subprocess.SubprocessError) as exc:
                failed = True
                result["service_start_succeeded"] = False
                result["recovery_error"] = str(exc)
        save(path, result)
    if fault_started and not failed:
        recovery_start = time.monotonic()
        deadline = recovery_start + args.recovery_timeout
        target_number = lsn_number(result["catchup_target_lsn"])
        try:
            while True:
                try:
                    state = status(args.connector)
                except (OSError, ValueError, urllib.error.URLError):
                    state = None
                sample = record("recovery", state)
                if state and healthy(state) and lsn_number(sample["confirmed_flush_lsn"]) >= target_number:
                    result["catchup_seconds"] = round(time.monotonic() - recovery_start, 3)
                    result["connector_status_after"] = state
                    break
                if time.monotonic() >= deadline:
                    raise TimeoutError("connector atau WAL belum pulih dalam batas waktu")
                time.sleep(min(args.sample_interval, max(0, deadline - time.monotonic())))
        except KeyboardInterrupt:
            interrupted = failed = True
            result["error"] = "interrupted during recovery monitoring"
        except Exception as exc:
            failed = True
            result["error"] = str(exc)
    baseline = [s["backlog_bytes"] for s in result["samples"] if s["phase"] == "baseline"]
    fault = [s["backlog_bytes"] for s in result["samples"] if s["phase"] in ("fault", "fault_end")]
    if baseline and fault:
        result["comparison"] = {"baseline_max_wal_backlog_bytes": max(baseline),
                                "fault_max_wal_backlog_bytes": max(fault),
                                "difference_bytes": max(fault) - max(baseline)}
    result["status"] = "failed" if failed else "completed"
    result["finished_at"] = timestamp()
    save(path, result)
    print(f"Hasil: {path}")
    if failed:
        print(f"Eksperimen belum selesai: {result.get('error') or result.get('recovery_error')}", file=sys.stderr)
    return 130 if interrupted else 1 if failed else 0
