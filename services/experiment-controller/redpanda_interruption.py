"""Observe a bounded Redpanda broker outage on the selected dev stack."""
import fcntl
import json
from pathlib import Path
import re
import signal
import subprocess
import sys
import time
import uuid
from datetime import datetime, timezone

from traffic import CONFIG, ROOT, COMPOSE, positive_float
from debezium_recovery import compose, healthy, lsn_number, name, save, status, timestamp, wal

RUNS = Path(__file__).resolve().parent / "logs" / "redpanda-interruption" / "runs"
LOCK = Path(__file__).resolve().parent / "logs" / ".redpanda-interruption.lock"


def register_parser(scenarios):
    parser = scenarios.add_parser("redpanda-interruption", help="hentikan broker dan amati pemulihan")
    parser.add_argument("--duration", required=True, type=positive_float, help="lama gangguan dalam detik")
    parser.add_argument("--baseline-duration", type=positive_float, default=30)
    parser.add_argument("--recovery-timeout", type=positive_float, default=300)
    parser.add_argument("--sample-interval", type=positive_float, default=5)
    parser.add_argument("--connector", type=name, default="ecommerce-postgres-source")
    parser.add_argument("--slot", type=name, default="ecommerce_debezium_slot")
    parser.add_argument("--group", default=None, help="consumer group; default dibaca dari container consumer dev")
    parser.set_defaults(handler=run)


def inspect_service(service):
    ids = compose("ps", "--status", "running", "-q", service, capture_output=True).stdout.split()
    if len(ids) != 1:
        raise RuntimeError(f"service {service} dev harus memiliki tepat satu container berjalan")
    raw = subprocess.run(["docker", "inspect", ids[0]], check=True, text=True,
                         capture_output=True, timeout=30).stdout
    info = json.loads(raw)[0]
    labels = info["Config"]["Labels"]
    if labels.get("com.docker.compose.project") != "realtime-pipeline-dev" or labels.get("com.docker.compose.service") != service:
        raise RuntimeError(f"label {service} tidak sesuai stack dev")
    state = info["State"]
    return {"id": ids[0], "running": state["Running"], "health": state.get("Health", {}).get("Status"),
            "restart_count": info["RestartCount"]}


def group_status(group):
    raw = compose("exec", "-T", "redpanda", "rpk", "group", "describe", group,
                  "--format", "json", "-X", "brokers=redpanda:9092", capture_output=True).stdout
    data = json.loads(raw)
    if not isinstance(data, list) or len(data) != 1 or data[0].get("group_name") != group:
        raise ValueError("hasil consumer group tidak dikenali")
    group = data[0]
    partitions = group.get("partitions") or []
    if partitions and any(not isinstance(part.get("lag"), (int, float)) or part["lag"] < 0 for part in partitions):
        raise ValueError("consumer lag tidak tersedia untuk semua partisi")
    return group


def observe(args, phase):
    sample = {"timestamp": timestamp(), "phase": phase, "errors": {}}
    for key, fn in (("wal", lambda: wal(args.slot)),
                    ("connector", lambda: status(args.connector)),
                    ("consumer", lambda: inspect_service("consumer")),
                    ("group", lambda: group_status(args.group))):
        try:
            sample[key] = fn()
        except (OSError, ValueError, RuntimeError, subprocess.SubprocessError) as exc:
            sample[key] = None
            sample["errors"][key] = str(exc)
    return sample


def ready(sample, broker):
    group = sample.get("group") or {}
    consumer = sample.get("consumer") or {}
    return (broker.get("health") == "healthy" and healthy(sample.get("connector") or {})
            and consumer.get("running") is True and consumer.get("health") == "healthy"
            and group.get("state") == "Stable" and bool(group.get("partitions")))


def run(args):
    LOCK.parent.mkdir(parents=True, exist_ok=True)
    with LOCK.open("a+") as lock:
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            print("Eksperimen Redpanda lain sedang berjalan.", file=sys.stderr)
            return 1
        previous = signal.getsignal(signal.SIGTERM)
        def interrupt(signum, frame):
            raise KeyboardInterrupt
        signal.signal(signal.SIGTERM, interrupt)
        try:
            return execute(args)
        finally:
            signal.signal(signal.SIGTERM, previous)


def execute(args):
    run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ") + "-" + uuid.uuid4().hex[:8]
    path = RUNS / run_id / "redpanda-interruption-result.json"
    path.parent.mkdir(parents=True)
    result = {"run_id": run_id, "environment": "dev", "project": "realtime-pipeline-dev",
              "mechanism": "stop/start service redpanda", "started_at": timestamp(),
              "parameters": vars(args).copy(), "samples": [], "status": "preflight",
              "limitations": ["Lag tidak tersedia saat broker mati; null bukan nol.",
                              "Log adalah bukti respons, bukan penghitung pasti retry atau kehilangan event.",
                              "Status healthy dan offset tidak membuktikan data tiba di ClickHouse.",
                              "Pastikan workload stabil dan stack dev terisolasi dari production."]}
    result["parameters"].pop("handler", None)
    fault = False
    error = None
    interrupted = False
    def collect(phase):
        sample = observe(args, phase)
        result["samples"].append(sample)
        save(path, result)
        return sample
    try:
        save(path, result)
        broker = inspect_service("redpanda")
        inspect_service("postgres-source")
        inspect_service("debezium")
        if args.group is None:
            group = compose("exec", "-T", "consumer", "printenv", "GROUP_ID", capture_output=True).stdout
            args.group = group.removesuffix("\n").removesuffix("\r")
            if not args.group:
                raise RuntimeError("GROUP_ID pada consumer dev kosong")
            result["parameters"]["group"] = args.group
        initial = collect("baseline")
        if not ready(initial, broker) or initial.get("wal") is None or initial["group"].get("total_lag") is None:
            group = initial.get("group") or {}
            raise RuntimeError(f"preflight gagal: broker={broker.get('health')}, "
                               f"connector={'RUNNING' if healthy(initial.get('connector') or {}) else 'tidak sehat'}, "
                               f"consumer={(initial.get('consumer') or {}).get('health')}, "
                               f"group={args.group!r}, state={group.get('state')}, "
                               f"partitions={len(group.get('partitions') or [])}, errors={initial['errors']}")
        result["traffic_config"] = CONFIG.read_text(encoding="utf-8")
        result["status"] = "baseline"
        end = time.monotonic() + args.baseline_duration
        while time.monotonic() < end:
            time.sleep(min(args.sample_interval, max(0, end - time.monotonic())))
            collect("baseline")
        baseline_lags = [s["group"]["total_lag"] for s in result["samples"]
                         if s.get("group") and isinstance(s["group"].get("total_lag"), (int, float))]
        result["baseline_max_consumer_lag"] = max(baseline_lags)
        result["fault_started_at"] = timestamp()
        result["status"] = "fault"
        save(path, result)
        fault = True
        compose("stop", "redpanda")
        if compose("ps", "--status", "running", "-q", "redpanda", capture_output=True).stdout.strip():
            raise RuntimeError("broker masih berjalan setelah stop")
        result["fault_verified"] = True
        save(path, result)
        end = time.monotonic() + args.duration
        while time.monotonic() < end:
            collect("fault")
            time.sleep(min(args.sample_interval, max(0, end - time.monotonic())))
        fault_end = collect("fault_end")
        if not fault_end.get("wal"):
            raise RuntimeError("posisi WAL target tidak tersedia")
        result["catchup_target_lsn"] = fault_end["wal"]["current_lsn"]
    except KeyboardInterrupt:
        error, interrupted = "interrupted", True
    except Exception as exc:
        error = str(exc)
    finally:
        if fault:
            result["recovery_started_at"] = timestamp()
            result["recovery_attempted"] = True
            try:
                compose("start", "redpanda")
                result["service_start_succeeded"] = True
            except (OSError, subprocess.SubprocessError) as exc:
                result["service_start_succeeded"] = False
                result["recovery_error"] = str(exc)
                error = error or str(exc)
        save(path, result)
    if fault and not error:
        start = time.monotonic()
        deadline = start + args.recovery_timeout
        try:
            while True:
                sample = collect("recovery")
                try:
                    broker = inspect_service("redpanda")
                except (OSError, ValueError, RuntimeError, subprocess.SubprocessError):
                    broker = {}
                group = sample.get("group") or {}
                current_wal = sample.get("wal") or {}
                caught_up = (current_wal.get("confirmed_flush_lsn") and
                             lsn_number(current_wal["confirmed_flush_lsn"]) >= lsn_number(result["catchup_target_lsn"]) and
                             isinstance(group.get("total_lag"), (int, float)) and
                             group["total_lag"] <= result["baseline_max_consumer_lag"])
                if ready(sample, broker) and caught_up:
                    result["recovery_condition"] = "connector RUNNING, WAL confirmed ke target, dan consumer lag <= maksimum baseline"
                    result["pipeline_ready_seconds"] = round(time.monotonic() - start, 3)
                    result["broker_after"] = broker
                    break
                if time.monotonic() >= deadline:
                    raise TimeoutError("pipeline belum kembali sehat dalam batas waktu")
                time.sleep(min(args.sample_interval, max(0, deadline - time.monotonic())))
        except KeyboardInterrupt:
            error, interrupted = "interrupted during recovery monitoring", True
        except Exception as exc:
            error = str(exc)
    if fault:
        for service in ("debezium", "consumer"):
            try:
                logs = compose("logs", "--no-color", "--timestamps", "--since", result["fault_started_at"], service,
                               capture_output=True).stdout
                (path.parent / f"{service}.log").write_text(logs, encoding="utf-8")
                result.setdefault("log_observations", {})[service] = {
                    "lines": len(logs.splitlines()),
                    "error_lines": sum(bool(re.search(r"\b(error|exception|failed|fatal)\b", line, re.I)) for line in logs.splitlines()),
                    "retry_lines": sum(bool(re.search(r"\b(retry|retrying|retries)\b", line, re.I)) for line in logs.splitlines())}
            except (OSError, subprocess.SubprocessError) as exc:
                result.setdefault("log_errors", {})[service] = str(exc)
    result["status"] = "failed" if error else "completed"
    result["error"] = error
    result["finished_at"] = timestamp()
    save(path, result)
    print(f"Hasil: {path}")
    if error:
        print(f"Eksperimen belum selesai: {error}", file=sys.stderr)
    return 130 if interrupted else 1 if error else 0
