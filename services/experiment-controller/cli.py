"""Entry point for non-production pipeline experiments."""

import argparse
import sys

import traffic
import postgres
import debezium
import redpanda
import consumer
import clickhouse
import debezium_recovery


def main(argv=None):
    parser = argparse.ArgumentParser(description="Kontrol eksperimen pipeline non-production")
    parser.add_argument("--environment", required=True, choices=["dev"],
                        help="stack non-production yang dipilih")
    commands = parser.add_subparsers(dest="command", required=True)
    traffic.register_parser(commands)
    postgres.register_parser(commands)
    debezium.register_parser(commands)
    redpanda.register_parser(commands)
    consumer.register_parser(commands)
    clickhouse.register_parser(commands)
    debezium_recovery.register_parser(commands)
    args = parser.parse_args(argv)
    return args.handler(args)


if __name__ == "__main__":
    sys.exit(main())
