"""Control the traffic generator on the explicitly selected development stack."""

import argparse
import math
from pathlib import Path
import subprocess
import sys

import yaml

ROOT = Path(__file__).resolve().parents[2]
CONFIG = ROOT / "services/order-service/traffic-config.dev.yaml"
COMPOSE = ["docker", "compose", "--env-file", str(ROOT / ".env"),
           "-p", "realtime-pipeline-dev", "-f", str(ROOT / "deployments/docker/docker-compose.yml"),
           "-f", str(ROOT / "deployments/docker/docker-compose.dev.yml")]


def positive_float(value):
    try:
        number = float(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError("harus berupa angka") from exc
    if not math.isfinite(number) or number <= 0:
        raise argparse.ArgumentTypeError("harus lebih besar dari nol")
    return number


def positive_int(value):
    try:
        number = int(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError("harus berupa bilangan bulat") from exc
    if number <= 0:
        raise argparse.ArgumentTypeError("harus lebih besar dari nol")
    return number


def update_config(args):
    with CONFIG.open(encoding="utf-8") as file:
        config = yaml.safe_load(file)
    fields = {"tps": ("request", "base_rate_per_second"),
              "max_concurrency": ("request", "max_concurrency"),
              "timeout": ("request", "timeout_seconds"),
              "duration": ("experiment", "duration_seconds"),
              "flash_sale": ("flash_sale", "enabled"),
              "flash_tps": ("flash_sale", "rate_per_second"),
              "flash_interval": ("flash_sale", "interval_seconds"),
              "flash_duration": ("flash_sale", "duration_seconds")}
    for name, (section, key) in fields.items():
        value = getattr(args, name)
        if value is not None:
            config[section][key] = value
    if args.unlimited:
        config["experiment"]["duration_seconds"] = None
    flash = config["flash_sale"]
    if flash["duration_seconds"] > flash["interval_seconds"]:
        raise ValueError("durasi flash sale tidak boleh melebihi interval")
    # Write in place: Docker bind mounts retain the same inode across updates.
    with CONFIG.open("w", encoding="utf-8") as file:
        yaml.safe_dump(config, file, sort_keys=False, allow_unicode=True)
    print("Konfigurasi dev diperbarui.")


def add_config_options(parser):
    parser.add_argument("--tps", type=positive_float)
    duration = parser.add_mutually_exclusive_group()
    duration.add_argument("--duration", type=positive_float, help="durasi eksperimen dalam detik")
    duration.add_argument("--unlimited", action="store_true", help="berjalan sampai dihentikan")
    parser.add_argument("--max-concurrency", type=positive_int)
    parser.add_argument("--timeout", type=positive_float)
    parser.add_argument("--flash-sale", action=argparse.BooleanOptionalAction, default=None)
    parser.add_argument("--flash-tps", type=positive_float)
    parser.add_argument("--flash-interval", type=positive_float)
    parser.add_argument("--flash-duration", type=positive_float)


def has_config_options(args):
    return any((args.tps, args.duration, args.unlimited, args.max_concurrency,
                args.timeout, args.flash_sale is not None, args.flash_tps,
                args.flash_interval, args.flash_duration))


def register_parser(commands):
    traffic = commands.add_parser("traffic", help="kontrol traffic generator")
    commands = traffic.add_subparsers(dest="traffic_command", required=True)
    for name in ("start", "restart"):
        command = commands.add_parser(name, help=f"{name} traffic generator dev")
        command.add_argument("config_keyword", nargs="?", choices=["config"],
                             help="opsional: awali pengaturan dengan kata config")
        add_config_options(command)
    commands.add_parser("stop", help="stop traffic generator dev")
    config = commands.add_parser("config", help="ubah konfigurasi traffic generator dev")
    add_config_options(config)
    traffic.set_defaults(handler=run)


def run(args):
    try:
        if args.traffic_command == "stop":
            subprocess.run(COMPOSE + ["stop", "traffic-generator"], cwd=ROOT, check=True)
            return 0
        changed = has_config_options(args)
        if args.traffic_command == "config" and not changed:
            raise ValueError("config memerlukan setidaknya satu pengaturan")
        if getattr(args, "config_keyword", None) and not changed:
            raise ValueError("config memerlukan setidaknya satu pengaturan")
        if changed:
            update_config(args)
        if args.traffic_command == "config":
            print("Restart generator agar perubahan berlaku.")
        else:
            operation = {"start": ["up", "-d", "--no-deps"],
                         "restart": ["restart"]}[args.traffic_command]
            if args.traffic_command == "start" and changed:
                operation.append("--force-recreate")
            subprocess.run(COMPOSE + operation + ["traffic-generator"], cwd=ROOT, check=True)
    except (OSError, ValueError, yaml.YAMLError, subprocess.CalledProcessError) as exc:
        print(f"Gagal: {exc}", file=sys.stderr)
        return 1
    return 0
