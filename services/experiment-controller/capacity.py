"""Capture a bounded, non-fault capacity observation on the dev Compose stack."""
import csv
import fcntl
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import time
import uuid
from datetime import datetime, timezone

import yaml
from traffic import COMPOSE, ROOT, CONFIG, positive_float
from debezium_recovery import save, timestamp
from redpanda_interruption import group_status, inspect_service

RUNS = Path(__file__).resolve().parent / "logs" / "capacity" / "runs"
LOCK = Path(__file__).resolve().parent / "logs" / ".capacity.lock"
CSV = ROOT / "services/order-service/logs/traffic.csv"
SERVICES = ("postgres-source", "order-service", "debezium", "redpanda", "consumer", "clickhouse")


def register_parser(scenarios):
    parser = scenarios.add_parser("capacity", help="jalankan pengamatan kapasitas dev tanpa fault")
    parser.add_argument("--tps", type=positive_float, required=True)
    parser.add_argument("--duration", type=positive_float, required=True)
    parser.add_argument("--sample-interval", type=positive_float, default=15)
    parser.add_argument("--kind", choices=("baseline", "steady", "spike"), default="steady")
    parser.add_argument("--flash-tps", type=positive_float)
    parser.add_argument("--flash-interval", type=positive_float, default=30)
    parser.add_argument("--flash-duration", type=positive_float, default=5)
    parser.set_defaults(handler=run)


def command(*args, **kwargs):
    return subprocess.run(COMPOSE + list(args), cwd=ROOT, text=True, check=True, timeout=30, **kwargs)


def find_summary(run_id):
    if not CSV.exists():
        return None
    with CSV.open(newline="", encoding="utf-8") as file:
        fcntl.flock(file, fcntl.LOCK_SH)
        return next((row for row in csv.DictReader(file) if row.get("run_id") == run_id), None)


def run(args):
    LOCK.parent.mkdir(parents=True, exist_ok=True)
    with LOCK.open("a+") as lock:
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            print("Capacity run lain masih berjalan.", file=sys.stderr)
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
    folder = RUNS / run_id
    folder.mkdir(parents=True)
    result_path = folder / "capacity-result.json"
    name = "capacity-" + run_id.lower()
    result = {"run_id": run_id, "environment": "dev", "project": "realtime-pipeline-dev", "kind": args.kind,
              "started_at": timestamp(), "status": "preflight", "samples": [],
              "limitations": ["CPU dan memori adalah snapshot docker stats; bukan profil penuh resource.",
                              "Latensi traffic mencakup request gagal dan timeout; persentil memakai nearest-rank.",
                              "Sustainable capacity memerlukan beberapa run stabil dan ambang yang ditentukan operator.",
                              "Generator lain di luar project Compose tidak dapat dideteksi otomatis."]}
    process = None
    error = None
    interrupted = False
    try:
        save(result_path, result)
        if args.flash_tps and args.flash_duration > args.flash_interval:
            raise ValueError("durasi flash sale tidak boleh melebihi interval")
        if args.kind == "spike" and args.flash_tps is None:
            raise ValueError("spike memerlukan --flash-tps")
        if args.kind != "spike" and args.flash_tps is not None:
            raise ValueError("--flash-tps hanya untuk --kind spike")
        active = command("ps", "--status", "running", "-q", "traffic-generator", capture_output=True).stdout.strip()
        if active:
            raise RuntimeError("traffic-generator dev sudah berjalan; hentikan sebelum capacity run")
        containers = {service: inspect_service(service) for service in SERVICES}
        ids = [v["id"] for v in containers.values()]
        inspected = subprocess.run(["docker", "inspect", *ids], check=True, capture_output=True,
                                   text=True, timeout=30).stdout
        constraints = {}
        for item in json.loads(inspected):
            service = item["Config"]["Labels"]["com.docker.compose.service"]
            host = item["HostConfig"]
            constraints[service] = {"image_id": item["Image"], "memory_bytes": host.get("Memory"),
                                    "nano_cpus": host.get("NanoCpus"), "cpu_quota": host.get("CpuQuota"),
                                    "cpu_period": host.get("CpuPeriod"), "cpuset_cpus": host.get("CpusetCpus")}
        result["resource_constraints"] = constraints
        consumer_id = containers["consumer"]["id"]
        env = subprocess.run(["docker", "inspect", "--format", '{{range .Config.Env}}{{println .}}{{end}}', consumer_id],
                             check=True, capture_output=True, text=True, timeout=30).stdout
        groups = [line[len("GROUP_ID="):] for line in env.splitlines() if line.startswith("GROUP_ID=")]
        if len(groups) != 1 or not groups[0]:
            raise RuntimeError("GROUP_ID consumer tidak tersedia")
        group = groups[0]
        config = yaml.safe_load(CONFIG.read_text(encoding="utf-8"))
        config["request"]["base_rate_per_second"] = args.tps
        config["experiment"]["duration_seconds"] = args.duration
        config["flash_sale"].update(enabled=args.kind == "spike", rate_per_second=args.flash_tps or config["flash_sale"]["rate_per_second"],
                                    interval_seconds=args.flash_interval, duration_seconds=args.flash_duration)
        config_path = folder / "traffic-config.yaml"
        config_path.write_text(yaml.safe_dump(config, sort_keys=False), encoding="utf-8")
        result.update(config=config, services=containers, consumer_group=group, status="running")
        save(result_path, result)
        with (folder / "traffic.log").open("w", encoding="utf-8") as logs:
            process = subprocess.Popen(COMPOSE + ["run", "--rm", "--no-deps", "--name", name,
                                       "-v", f"{config_path}:/app/traffic-config.yaml:ro",
                                       "-e", f"TRAFFIC_RUN_ID={run_id}", "traffic-generator"],
                                       cwd=ROOT, stdout=logs, stderr=subprocess.STDOUT)
            deadline = time.monotonic() + args.duration + max(60, float(config["request"]["timeout_seconds"]) + 30)
            while process.poll() is None:
                if time.monotonic() >= deadline:
                    raise TimeoutError("generator melewati batas durasi")
                sample = {"timestamp": timestamp(), "resources": None, "consumer_group": None, "errors": {}}
                try:
                    ids = [v["id"] for v in containers.values()]
                    out = subprocess.run(["docker", "stats", "--no-stream", "--format", "{{json .}}", *ids],
                                         check=True, text=True, capture_output=True, timeout=30).stdout
                    sample["resources"] = [json.loads(line) for line in out.splitlines() if line]
                except (OSError, ValueError, subprocess.SubprocessError) as exc:
                    sample["errors"]["resources"] = str(exc)
                try:
                    sample["consumer_group"] = group_status(group)
                except (OSError, ValueError, subprocess.SubprocessError) as exc:
                    sample["errors"]["consumer_group"] = str(exc)
                result["samples"].append(sample)
                save(result_path, result)
                time.sleep(min(args.sample_interval, max(0, deadline - time.monotonic())))
            if process.returncode != 0:
                raise RuntimeError(f"generator keluar dengan kode {process.returncode}")
        result["traffic_summary"] = find_summary(run_id)
        if not result["traffic_summary"]:
            raise RuntimeError("ringkasan traffic untuk run ID tidak ditemukan; periksa image dan traffic.log")
    except KeyboardInterrupt:
        error, interrupted = "interrupted", True
    except Exception as exc:
        error = str(exc)
    finally:
        if process is not None and process.poll() is None:
            try:
                subprocess.run(["docker", "stop", "--time", "15", name], check=True, capture_output=True, text=True, timeout=25)
            except (OSError, subprocess.SubprocessError) as exc:
                result["cleanup_error"] = str(exc)
                error = error or str(exc)
            try:
                process.wait(timeout=20)
            except subprocess.TimeoutExpired:
                process.terminate()
                error = error or "proses Compose tidak berhenti"
        result["finished_at"] = timestamp()
        result["status"] = "failed" if error else "completed"
        result["error"] = error
        save(result_path, result)
    print(f"Hasil: {result_path}")
    if error:
        print(f"Capacity run gagal: {error}", file=sys.stderr)
    return 130 if interrupted else 1 if error else 0
