# 1. Load environment variables
ENV_FILE ?= .env
ifneq (,$(wildcard $(ENV_FILE)))
    include $(ENV_FILE)
    export
endif

# Variables
DOCKER_COMPOSE ?= docker compose --env-file .env -f deployments/docker/docker-compose.yml
DOCKER_COMPOSE_DEV ?= docker compose --env-file .env -p realtime-pipeline-dev -f deployments/docker/docker-compose.yml -f deployments/docker/docker-compose.dev.yml
DEBEZIUM_PORT ?= 8083
GRAFANA_BASE_URL ?= http://localhost:3000
POSTGRES_USER ?= admin
POSTGRES_DB ?= ecom_db
POSTGRES_HOST ?= postgres-source
POSTGRES_PORT ?= 5432
CONNECTOR_NAME ?= ecommerce-postgres-source
TOPIC_PREFIX ?= ecommerce
SLOT_NAME ?= ecommerce_debezium_slot
PUBLICATION_NAME ?= ecommerce_debezium_publication
TABLE_INCLUDE_LIST ?= public.users,public.products,public.orders,public.order_items
export POSTGRES_USER POSTGRES_DB POSTGRES_HOST POSTGRES_PORT CONNECTOR_NAME TOPIC_PREFIX SLOT_NAME PUBLICATION_NAME TABLE_INCLUDE_LIST
export GRAFANA_BASE_URL

.PHONY: help build test clean docker-up docker-down docker-build order-service-bash db-shell init-db clean-db logs export-dashboard export-dashboard-script logs-cdc logs-tg logs-consumer init-redpanda init-debezium init-clickhouse init-analytics init-superset clean-clickhouse reset-clickhouse drop-slot seed-db generate-traffic resume stop reset-all act-deploy k8s-db-shell
.PHONY: docker-up-dev docker-down-dev docker-build-dev docker-down-v-dev docker-rebuild-dev

help:
	@echo "Enterprise CDC Pipeline - Available Commands"
	@echo ""
	@echo "  make build              Build Go applications"
	@echo "  make docker-up          Start all services (with build check)"
	@echo "  make docker-down        Stop and remove containers"
	@echo "  make docker-rebuild     Force rebuild and restart order-service"
	@echo "  make docker-up-dev      Start development services"
	@echo "  make docker-down-dev    Stop and remove development containers"
	@echo "  make docker-build-dev   Build development images"
	@echo "  make docker-down-v-dev  Stop development services and remove their volumes"
	@echo "  make docker-rebuild-dev Force rebuild and restart development order-service"
	@echo "  make init-redpanda      Initialize Redpanda topics with specific partitions"
	@echo "  make init-clickhouse    Initialize ClickHouse schema (Bronze Layer)"
	@echo "  make init-analytics     Initialize ClickHouse Analytics schema (Silver & Gold OBT)"
	@echo "  make init-superset      Initialize Apache Superset (DB & Admin User)"
	@echo "  make clean-clickhouse   Drop all ClickHouse history tables and views"
	@echo "  make reset-clickhouse   Clean and Re-initialize ClickHouse (Reset Everything)"
	@echo "  make init-db            Initialize database schema (create tables)"
	@echo "  make seed-db            Mass insert 1000s of dummy users and products"
	@echo "  make generate-traffic   Trigger infinite random orders (CDC load testing)"
	@echo "  make clean-db           Drop all existing tables in the database"
	@echo "  make db-shell           Enter PostgreSQL CLI"
	@echo "  make order-service-bash Enter FastAPI container"
	@echo "  make clickhouse-shell   Enter ClickHouse CLI"
	@echo "  make logs               View all container logs"
	@echo "  Add -dev to service targets to use the development stack (e.g. make db-shell-dev)"

build:
	@echo "Building Go applications...."
	mkdir -p bin
	go build -o bin/producer cmd/producer/main.go
	go build -o bin/processor cmd/processor/main.go

docker-build:
	@echo "Building Docker images..."
	$(DOCKER_COMPOSE) build

docker-up:
	@echo "Starting services..."
	$(DOCKER_COMPOSE) up -d

docker-down:
	@echo "Stopping services..."
	$(DOCKER_COMPOSE) down

docker-down-v:
	@echo "Stopping services and removing volumes..."
	$(DOCKER_COMPOSE) down -v

docker-build-dev:
	$(DOCKER_COMPOSE_DEV) build

docker-up-dev:
	$(DOCKER_COMPOSE_DEV) up -d

docker-down-dev:
	$(DOCKER_COMPOSE_DEV) down

docker-down-v-dev:
	$(DOCKER_COMPOSE_DEV) down -v

docker-rebuild-dev:
	$(DOCKER_COMPOSE_DEV) build --no-cache order-service
	$(DOCKER_COMPOSE_DEV) up -d order-service

# Perintah khusus untuk reset jika order-service error terus (Clear Cache)
docker-rebuild:
	@echo "Rebuilding order-service without cache..."
	$(DOCKER_COMPOSE) build --no-cache order-service
	$(DOCKER_COMPOSE) up -d order-service

order-service-bash:
	@echo "Entering order-service container..."
	$(DOCKER_COMPOSE) exec order-service /bin/bash

init-all:
	$(MAKE) init-redpanda init-db seed-db init-clickhouse init-dbz
	init-superset

init-all-dev:
	$(MAKE) init-redpanda-dev init-db-dev seed-db-dev init-clickhouse-dev init-dbz-dev
	init-superset-dev

init-redpanda:
	@echo "Initializing Redpanda Topics..."
	@echo "Waiting for Redpanda to be ready..."
	@sleep 5
	$(DOCKER_COMPOSE) exec -T redpanda rpk topic describe cdc-events >/dev/null 2>&1 || $(DOCKER_COMPOSE) exec -T redpanda rpk topic create cdc-events -p 3
	@echo "✓ Topic cdc-events with 3 partitions initialized"

init-dbz:
	@echo "Generating connector config..."
	@envsubst < connectors/postgres-template.json > connectors/postgres-source.json
	@echo "Registering Debezium connector..."
	@for attempt in $$(seq 1 60); do curl -fsS http://localhost:$(DEBEZIUM_PORT)/connectors >/dev/null 2>&1 && break; if [ "$$attempt" -eq 60 ]; then echo "Debezium is not ready"; exit 1; fi; sleep 2; done
	@if curl -fsS "http://localhost:$(DEBEZIUM_PORT)/connectors/$(CONNECTOR_NAME)" >/dev/null 2>&1; then \
		echo "Connector $(CONNECTOR_NAME) already exists"; \
	else \
		curl --fail-with-body -sS -X POST http://localhost:$(DEBEZIUM_PORT)/connectors \
			-H "Content-Type: application/json" --data @connectors/postgres-source.json; \
	fi
	@echo "\nDebezium connector registration request succeeded"

init-db:
	@echo "Initializing database schema..."
	@echo "Waiting for PostgreSQL to be ready..."
	@for attempt in $$(seq 1 60); do $(DOCKER_COMPOSE) exec -T postgres-source pg_isready -U $(POSTGRES_USER) -d $(POSTGRES_DB) >/dev/null 2>&1 && break; if [ "$$attempt" -eq 60 ]; then echo "PostgreSQL is not ready"; exit 1; fi; sleep 2; done
	$(DOCKER_COMPOSE) exec -T postgres-source psql -v ON_ERROR_STOP=1 -U $(POSTGRES_USER) -d $(POSTGRES_DB) -f /dev/stdin < scripts/sql/init_source.sql
	@echo "✓ Database schema initialized successfully"

seed-db:
	@echo "Suntik Data Massal (Seeding) sedang berjalan..."
	$(DOCKER_COMPOSE) exec -T order-service python data_generator.py --seed

generate-traffic:
	@echo "Mengaktifkan Robot Transaksi di background..."
	$(DOCKER_COMPOSE) exec -d order-service python data_generator.py --traffic

clean-db:
	@echo "Dropping existing database tables..."
	$(DOCKER_COMPOSE) exec -T postgres-source psql -U $(POSTGRES_USER) -d $(POSTGRES_DB) -c "DROP TABLE IF EXISTS order_items, orders, products, users CASCADE;"
	@echo "✓ Database tables dropped successfully"

db-shell:
	@echo "Accessing PostgreSQL shell..."
	$(DOCKER_COMPOSE) exec postgres-source psql -U $(POSTGRES_USER) -d $(POSTGRES_DB)

clickhouse-shell:
	@echo "Accessing ClickHouse shell..."
	$(DOCKER_COMPOSE) exec clickhouse clickhouse-client --password $(CLICKHOUSE_ADMIN_PASSWORD)

init-clickhouse:
	@echo "Initializing ClickHouse schema..."
	@for attempt in $$(seq 1 60); do $(DOCKER_COMPOSE) exec -T clickhouse clickhouse-client --password $(CLICKHOUSE_ADMIN_PASSWORD) --query 'SELECT 1' >/dev/null 2>&1 && break; if [ "$$attempt" -eq 60 ]; then echo "ClickHouse is not ready"; exit 1; fi; sleep 2; done
	$(DOCKER_COMPOSE) exec -T clickhouse clickhouse-client --password $(CLICKHOUSE_ADMIN_PASSWORD) --multiquery < scripts/sql/init_clickhouse.sql
	$(DOCKER_COMPOSE) exec -T clickhouse clickhouse-client --password $(CLICKHOUSE_ADMIN_PASSWORD) --multiquery < scripts/sql/init_analytics.sql
	@echo "✓ ClickHouse schema initialized successfully"

init-superset:
	@echo "Initializing Apache Superset..."
	$(DOCKER_COMPOSE) exec -T superset bash < deployments/docker/init-superset.sh
	@echo "✓ Superset is initialized and ready to use"

reset-all: drop-slot clean-db clean-clickhouse init-db init-clickhouse init-analytics init-redpanda
	@echo "🚀 FULL SYSTEM RESET COMPLETE"

# Perintah untuk melanjutkan pekerjaan tanpa menghapus Dashboard/Data
resume:
	@echo "🎬 RESUMING ALL SERVICES..."
	$(DOCKER_COMPOSE) up -d
	@echo "🚀 RESTARTING INGESTOR..."
	$(DOCKER_COMPOSE) restart debezium traffic-generator
	@echo "✅ SERVICES ARE RESUMED"
	$(MAKE) logs

# Matikan tanpa hapus data
stop:
	@echo "🛑 STOPPING SERVICES (Data is safe)..."
	$(DOCKER_COMPOSE) stop

# Alias untuk inisialisasi lengkap
setup-db: init-db seed-db


export-dashboard:
	@echo "📤 Exporting dashboard from Dev to provisioning..."
	python3 -m scripts.export-dashboard
	@echo "✓ Dashboard export complete. Now reloading"
	curl -sf -X POST -u "$(GRAFANA_ADMIN_USER):$(GRAFANA_ADMIN_PASSWORD)" $(GRAFANA_BASE_URL)/api/admin/provisioning/dashboards/reload

logs:
	$(DOCKER_COMPOSE) logs -f

logs-cdc:
	$(DOCKER_COMPOSE) logs -f debezium

logs-tg:
	# tail 20 lines of logs and follow
	$(DOCKER_COMPOSE) logs --tail=20 --follow traffic-generator

logs-consumer:
	# tail 20 lines of logs and follow
	$(DOCKER_COMPOSE) logs --tail=20 --follow consumer

build-restart-tg:
	@echo "Building and restarting traffic-generator..."
	$(DOCKER_COMPOSE) build traffic-generator
	$(DOCKER_COMPOSE) up -d traffic-generator

restart-tg:
	@echo "Restarting traffic-generator..."
	$(DOCKER_COMPOSE) restart traffic-generator

pause-tg:
	@echo "Pausing traffic-generator..."
	$(DOCKER_COMPOSE) stop traffic-generator

resume-tg:
	@echo "Starting traffic-generator..."
	$(DOCKER_COMPOSE) start traffic-generator

docker-logs-os:
	$(DOCKER_COMPOSE) logs order-service

# Jalankan target service yang sama dengan proyek dan port development.
DEV_SERVICE_TARGETS := order-service-bash init-redpanda init-dbz init-db seed-db generate-traffic clean-db db-shell clickhouse-shell init-clickhouse init-superset resume stop setup-db export-dashboard logs logs-cdc logs-tg logs-consumer restart-tg pause-tg resume-tg build-restart-tg docker-logs-os
.PHONY: $(addsuffix -dev,$(DEV_SERVICE_TARGETS))
$(addsuffix -dev,$(DEV_SERVICE_TARGETS)):
	$(MAKE) $(@:-dev=) DOCKER_COMPOSE='$(DOCKER_COMPOSE_DEV)' DEBEZIUM_PORT='$(DEBEZIUM_DEV_PORT)' GRAFANA_BASE_URL='http://localhost:$(GRAFANA_DEV_PORT)'

clean:
	@echo "Cleaning build artifacts..."
	rm -rf bin/
	go clean

act-deploy:
	act -P ubuntu-latest=catthehacker/ubuntu:act-latest --secret-file .secrets --network bridge


# ========================= K8S =========================
k8s-db-shell:
	minikube kubectl -- exec -it postgres-source-0 -- psql -U $(POSTGRES_USER) -d $(POSTGRES_DB)
