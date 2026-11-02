"""Measure consumer backlog and reconcile order creation during a dev outage."""
import fcntl
import json
from pathlib import Path
import signal
import subprocess
import sys
import time
import uuid
from datetime import datetime, timezone

from traffic import CONFIG, positive_float
from debezium_recovery import compose, healthy, name, save, status, timestamp
from redpanda_interruption import group_status, inspect_service

RUNS = Path(__file__).resolve().parent / "logs" / "consumer-recovery" / "runs"
LOCK = Path(__file__).resolve().parent / "logs" / ".consumer-recovery.lock"


def register_parser(scenarios):
    parser = scenarios.add_parser("consumer-recovery", help="ukur backlog dan pemulihan consumer dev")
    parser.add_argument("--duration", required=True, type=positive_float)
    parser.add_argument("--baseline-duration", type=positive_float, default=30)
    parser.add_argument("--recovery-timeout", type=positive_float, default=300)
    parser.add_argument("--sample-interval", type=positive_float, default=5)
    parser.add_argument("--connector", type=name, default="ecommerce-postgres-source")
    parser.add_argument("--group", default=None)
    parser.set_defaults(handler=run)


def source_orders(after):
    sql = f"SELECT id FROM orders WHERE id > {int(after)} ORDER BY id"
    output = compose("exec", "-T", "postgres-source", "sh", "-c",
                     'exec psql -X -v ON_ERROR_STOP=1 -U "$POSTGRES_USER" -d "$POSTGRES_DB" -At -c "$1"',
                     "sh", sql, capture_output=True).stdout
    return [int(line) for line in output.splitlines() if line.strip()]


def source_max():
    output = compose("exec", "-T", "postgres-source", "sh", "-c",
                     'exec psql -X -v ON_ERROR_STOP=1 -U "$POSTGRES_USER" -d "$POSTGRES_DB" -At -c "SELECT COALESCE(MAX(id), 0) FROM orders"',
                     capture_output=True).stdout.strip()
    return int(output)


def reconcile(ids):
    if not ids:
        return {"status": "inconclusive", "reason": "tidak ada order baru pada jendela pengamatan"}
    counts = {}
    # Split queries to avoid an unbounded command-line argument.
    for start in range(0, len(ids), 500):
        selected = ids[start:start + 500]
        query = ("SELECT id, count() FROM orders_history WHERE operation = 'c' AND id IN (" +
                 ",".join(str(int(i)) for i in selected) + ") GROUP BY id FORMAT TabSeparated")
        result = compose("exec", "-T", "clickhouse", "sh", "-c",
                         'exec clickhouse-client --user default --password "$CLICKHOUSE_ADMIN_PASSWORD" --query "$1"',
                         "sh", query, capture_output=True)
        for line in result.stdout.splitlines():
            key, count = line.split("\t")
            counts[int(key)] = int(count)
    missing = [i for i in ids if counts.get(i, 0) == 0]
    duplicates = {str(i): counts[i] for i in ids if counts.get(i, 0) > 1}
    return {"status": "complete", "source_orders": len(ids), "missing_order_ids": missing,
            "duplicate_create_counts": duplicates,
            "limitations": "Hanya order yang masih ada di sumber dan event create; update/delete, tabel lain, dan transaksi yang commit terlambat tidak diverifikasi."}


def partition_offsets(group, field):
    parts = group.get("partitions") or []
    if not parts:
        raise ValueError("partisi consumer group tidak tersedia")
    result = {}
    for part in parts:
        value = part.get(field)
        if not isinstance(value, int) or value < 0:
            raise ValueError(f"{field} tidak tersedia untuk {part.get('topic')} partisi {part.get('partition')}")
        result[f"{part['topic']}:{part['partition']}"] = value
    return result


def run(args):
    LOCK.parent.mkdir(parents=True, exist_ok=True)
    with LOCK.open("a+") as lock:
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            print("Eksperimen consumer lain sedang berjalan.", file=sys.stderr)
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
    path = RUNS / run_id / "consumer-recovery-result.json"
    path.parent.mkdir(parents=True)
    result = {"run_id": run_id, "environment": "dev", "project": "realtime-pipeline-dev",
              "started_at": timestamp(), "mechanism": "stop/start Compose service consumer",
              "parameters": {k: v for k, v in vars(args).items() if k != "handler"},
              "samples": [], "status": "preflight",
              "limitations": ["Rekonsiliasi hanya order create yang masih ada pada sumber; event update/delete dan tabel lain tidak diverifikasi.",
                              "ID berurutan dan snapshot sumber tidak menjamin transaksi yang commit melewati batas snapshot ikut tercatat.",
                              "Consumer lag adalah offset per partisi, bukan jumlah order atau durasi latency."]}
    fault = False
    error = None
    interrupted = False
    def sample(phase):
        group = group_status(args.group)
        data = {"timestamp": timestamp(), "phase": phase, "group": group}
        result["samples"].append(data)
        save(path, result)
        return data
    try:
        save(path, result)
        for service in ("postgres-source", "redpanda", "debezium", "consumer", "clickhouse"):
            service_state = inspect_service(service)
            if service in ("redpanda", "consumer") and service_state.get("health") != "healthy":
                raise RuntimeError(f"{service} belum healthy")
        if not healthy(status(args.connector)):
            raise RuntimeError("connector belum RUNNING")
        if args.group is None:
            args.group = compose("exec", "-T", "consumer", "printenv", "GROUP_ID", capture_output=True).stdout.removesuffix("\n").removesuffix("\r")
            if not args.group:
                raise RuntimeError("GROUP_ID consumer kosong")
            result["parameters"]["group"] = args.group
        first = sample("baseline")["group"]
        if first.get("state") != "Stable":
            raise RuntimeError("consumer group belum Stable")
        baseline_start = partition_offsets(first, "log_end_offset")
        result["traffic_config"] = CONFIG.read_text(encoding="utf-8")
        end = time.monotonic() + args.baseline_duration
        while time.monotonic() < end:
            time.sleep(min(args.sample_interval, max(0, end - time.monotonic())))
            last = sample("baseline")["group"]
        baseline_end = partition_offsets(last, "log_end_offset")
        if set(baseline_start) != set(baseline_end) or sum(baseline_end.values()) <= sum(baseline_start.values()):
            raise RuntimeError("tidak ada traffic masuk teramati selama baseline")
        result["source_order_id_before"] = source_max()
        result["fault_started_at"] = timestamp()
        result["status"] = "fault"
        save(path, result)
        fault = True
        compose("stop", "consumer")
        if compose("ps", "--status", "running", "-q", "consumer", capture_output=True).stdout.strip():
            raise RuntimeError("consumer masih berjalan setelah stop")
        result["fault_verified"] = True
        initial_fault = sample("fault")["group"]
        end = time.monotonic() + args.duration
        while time.monotonic() < end:
            time.sleep(min(args.sample_interval, max(0, end - time.monotonic())))
            final_fault = sample("fault")["group"]
        target = partition_offsets(final_fault, "log_end_offset")
        initial_offsets = partition_offsets(initial_fault, "log_end_offset")
        if set(target) != set(initial_offsets):
            raise RuntimeError("partisi berubah saat gangguan")
        result["target_offsets"] = target
        result["backlog_growth_offsets"] = sum(target[k] - initial_offsets[k] for k in target)
        ids = source_orders(result["source_order_id_before"])
        result["source_order_ids"] = ids
        if not ids or result["backlog_growth_offsets"] <= 0:
            raise RuntimeError("tidak ada order baru atau pertumbuhan backlog selama gangguan")
    except KeyboardInterrupt:
        interrupted, error = True, "interrupted"
    except Exception as exc:
        error = str(exc)
    finally:
        if fault:
            result["recovery_started_at"] = timestamp()
            result["recovery_attempted"] = True
            try:
                compose("start", "consumer")
                result["service_start_succeeded"] = True
            except (OSError, subprocess.SubprocessError) as exc:
                result["service_start_succeeded"] = False
                result["recovery_error"] = str(exc)
                error = error or str(exc)
        save(path, result)
    if fault and not error:
        started = time.monotonic()
        deadline = started + args.recovery_timeout
        try:
            while True:
                try:
                    group = sample("recovery")["group"]
                    offsets = partition_offsets(group, "current_offset")
                    if group.get("state") == "Stable" and all(offsets.get(k, -1) >= v for k, v in target.items()):
                        result["drain_seconds"] = round(time.monotonic() - started, 3)
                        break
                except (OSError, ValueError, subprocess.SubprocessError):
                    pass
                if time.monotonic() >= deadline:
                    raise TimeoutError("consumer belum mencapai offset target")
                time.sleep(min(args.sample_interval, max(0, deadline - time.monotonic())))
            result["reconciliation"] = reconcile(ids)
        except KeyboardInterrupt:
            interrupted, error = True, "interrupted during recovery"
        except Exception as exc:
            error = str(exc)
    result["status"] = "failed" if error else "completed"
    result["error"] = error
    result["finished_at"] = timestamp()
    save(path, result)
    print(f"Hasil: {path}")
    if error:
        print(f"Eksperimen belum selesai: {error}", file=sys.stderr)
    return 130 if interrupted else 1 if error else 0
