"""Reconcile order create events for a recorded dev experiment window."""
import argparse
from datetime import datetime, timezone
import json
import subprocess
import sys
from pathlib import Path
import time

from consumer_recovery import reconcile
from debezium_recovery import compose, save, timestamp
from redpanda_interruption import inspect_service
from traffic import positive_float


def register_parser(scenarios):
    parser = scenarios.add_parser("reconcile-orders", help="rekonsiliasi order sumber ke ClickHouse")
    parser.add_argument("--run", required=True, type=Path, help="path JSON run capacity atau recovery")
    parser.add_argument("--wait", type=positive_float, default=120, help="maksimum detik menunggu order di sink")
    parser.add_argument("--sample-interval", type=positive_float, default=5)
    parser.set_defaults(handler=run)


def utc(value):
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        raise ValueError("timestamp run harus memiliki zona waktu")
    return parsed.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M:%S.%f")


def source_ids(start, end):
    # Inputs are canonical UTC timestamps produced by utc(), not shell input.
    sql = ("SELECT id FROM orders WHERE created_at >= timestamp '" + utc(start) +
           "' AND created_at < timestamp '" + utc(end) + "' ORDER BY id")
    output = compose("exec", "-T", "postgres-source", "sh", "-c",
                     'exec psql -X -v ON_ERROR_STOP=1 -U "$POSTGRES_USER" -d "$POSTGRES_DB" -At -c "$1"',
                     "sh", sql, capture_output=True).stdout
    return [int(line) for line in output.splitlines() if line.strip()]


def run(args):
    try:
        doc = json.loads(args.run.read_text(encoding="utf-8"))
        if doc.get("environment") != "dev" or doc.get("status") != "completed":
            raise ValueError("run harus selesai pada environment dev")
        for service in ("postgres-source", "clickhouse"):
            inspect_service(service)
        zone = compose("exec", "-T", "postgres-source", "sh", "-c",
                       'exec psql -X -U "$POSTGRES_USER" -d "$POSTGRES_DB" -At -c "SHOW timezone"',
                       capture_output=True).stdout.strip()
        if zone.upper() not in ("UTC", "ETC/UTC"):
            raise ValueError(f"timezone PostgreSQL bukan UTC: {zone}; rekonsiliasi dibatalkan")
        traffic = doc.get("traffic_summary") or {}
        start = doc.get("fault_started_at") or traffic.get("started_at")
        end = doc.get("recovery_started_at") or traffic.get("timestamp")
        if not start or not end or utc(start) >= utc(end):
            raise ValueError("batas waktu run tidak tersedia atau tidak valid")
        ids = source_ids(start, end)
        result = {"run_id": doc["run_id"], "environment": "dev", "window_start_utc": start,
                  "window_end_utc": end, "checked_at": timestamp(), "source_orders": len(ids),
                  "limitations": ["Hanya order yang masih ada di sumber dan event create di ClickHouse.",
                                  "created_at adalah timestamp awal transaksi; transaksi lintas batas atau data yang dihapus dapat tidak tercakup.",
                                  "Lebih dari satu create adalah kandidat duplikasi; bukan identitas event CDC unik."]}
        deadline = time.monotonic() + args.wait
        while True:
            comparison = reconcile(ids)
            if comparison.get("status") != "complete" or not comparison["missing_order_ids"] or time.monotonic() >= deadline:
                break
            time.sleep(min(args.sample_interval, max(0, deadline - time.monotonic())))
        result["reconciliation"] = comparison
        result["finished_at"] = timestamp()
        target = args.run.parent / "orders-reconciliation.json"
        save(target, result)
        print(f"Hasil: {target}")
        print(f"Order sumber: {len(ids)}; belum ditemukan: {len(comparison.get('missing_order_ids', []))}; kandidat duplikat: {len(comparison.get('duplicate_create_counts', {}))}")
        return 0 if comparison.get("status") == "complete" and not comparison["missing_order_ids"] and not comparison["duplicate_create_counts"] else 1
    except (OSError, ValueError, KeyError, subprocess.SubprocessError) as exc:
        print(f"Rekonsiliasi gagal: {exc}", file=sys.stderr)
        return 1
