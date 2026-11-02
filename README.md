# Real-time CDC Pipeline (Postgres → ClickHouse)

<p align="center">
  <img src="https://img.shields.io/badge/PostgreSQL-16-4169E1?logo=postgresql&logoColor=white" />
  <img src="https://img.shields.io/badge/Debezium-3.6-000000?logo=debezium&logoColor=white" />
  <img src="https://img.shields.io/badge/Redpanda-26.1-EA1C2D?logo=redpanda&logoColor=white" />
  <img src="https://img.shields.io/badge/Go-Consumer-00ADD8?logo=go&logoColor=white" />
  <img src="https://img.shields.io/badge/ClickHouse-26.3-FFCC01?logo=clickhouse&logoColor=black" />
</p>

<p align="center">
  <img src="https://img.shields.io/badge/Apache_Superset-6.0-20A6C9?logo=apachesuperset&logoColor=white" />
  <img src="https://img.shields.io/badge/Prometheus-3.11-E6522C?logo=prometheus&logoColor=white" />
  <img src="https://img.shields.io/badge/Grafana-13.0-F46800?logo=grafana&logoColor=white" />
  <img src="https://img.shields.io/badge/Docker-Compose-2496ED?logo=docker&logoColor=white" />
</p>

Proyek ini adalah implementasi **real-time Change Data Capture (CDC)** dari simulasi database transaksional **E-Commerce** (PostgreSQL) ke ClickHouse. Setiap perubahan data (INSERT/UPDATE/DELETE) pada tabel `users`, `products`, `orders`, dan `order_items` langsung tertangkap, dikirim via Redpanda (Kafka), dan disimpan di ClickHouse sebagai **CDC Event Log** (history tracking) dengan medallion architecture (Bronze → Silver → Gold).

<br>

```mermaid
flowchart LR
    %% =========================
    %% Styles
    %% =========================
    classDef source fill:#2563EB,stroke:#1D4ED8,color:#FFFFFF,stroke-width:2px
    classDef cdc fill:#F97316,stroke:#EA580C,color:#FFFFFF,stroke-width:2px
    classDef broker fill:#7C3AED,stroke:#6D28D9,color:#FFFFFF,stroke-width:2px
    classDef processing fill:#0891B2,stroke:#0E7490,color:#FFFFFF,stroke-width:2px
    classDef storage fill:#EAB308,stroke:#CA8A04,color:#111827,stroke-width:2px
    classDef visualization fill:#16A34A,stroke:#15803D,color:#FFFFFF,stroke-width:2px
    classDef observability fill:#DC2626,stroke:#B91C1C,color:#FFFFFF,stroke-width:2px
    classDef operational fill:#475569,stroke:#334155,color:#FFFFFF,stroke-width:2px

    %% =========================
    %% Main data flow
    %% =========================
    subgraph sourceLayer["Source Layer"]
        api["Order Service<br/>FastAPI"]
        generator["Traffic Generator<br/>Faker"]
        postgres[("PostgreSQL 16<br/>Transactional Database")]

        api -->|"Create and update transactions"| postgres
        generator -->|"Generate synthetic traffic"| api
    end

    subgraph ingestionLayer["CDC Ingestion"]
        debezium["Debezium 3.6<br/>Kafka Connect"]
    end

    subgraph streamingLayer["Streaming Layer"]
        redpanda[("Redpanda<br/>Kafka-compatible Broker")]
        console["Redpanda Console"]
    end

    subgraph processingLayer["Stream Processing"]
        consumer["Go Consumer<br/>franz-go"]
    end

    subgraph analyticsLayer["Analytics Platform"]
        clickhouse[("ClickHouse<br/>CDC Event History")]
        medallion["Medallion Models<br/>Bronze → Silver → Gold"]
        superset["Apache Superset<br/>Analytics Dashboard"]
    end

    postgres -->|"WAL / logical replication"| debezium
    debezium -->|"Debezium JSON events"| redpanda
    redpanda -->|"Consumer group"| consumer
    consumer -->|"Validated batched inserts"| clickhouse
    clickhouse --> medallion
    medallion -->|"Analytical queries"| superset

    console -.->|"Inspect topics and messages"| redpanda

    %% =========================
    %% Observability
    %% =========================
    subgraph observabilityLayer["Observability"]
        nodeExporter["Node Exporter"]
        postgresExporter["Postgres Exporter"]
        prometheus["Prometheus"]
        grafana["Grafana"]

        nodeExporter --> prometheus
        postgresExporter --> prometheus
        prometheus --> grafana
    end

    postgres -.-> postgresExporter
    redpanda -.-> prometheus
    clickhouse -.-> prometheus

    class api,generator,postgres source
    class debezium cdc
    class redpanda broker
    class consumer processing
    class clickhouse,medallion storage
    class superset visualization
    class nodeExporter,postgresExporter,prometheus,grafana observability
    class console operational
```
<!-- ![System Architecture](docs/screenshots/ARCHITECTURE_DIAGRAM.png) -->
<br>

## Arsitektur

| Layer | Teknologi | Fungsi |
|-------|-----------|--------|
| **Source** | PostgreSQL (WAL) | Simulasi database transaksional E-Commerce (users, products, orders, order_items) |
| **Ingestion** | Debezium (Kafka Connect) | Kelola replication slot & logical decoding, kirim event standar Debezium ke Redpanda |
| **Message Broker** | Redpanda (Kafka) | Buffer pesan + Schema Registry (payload Debezium JSON) |
| **Warehouse** | ClickHouse | OLAP dengan CDC Event Logs + Materialized Views + Query-time Views (Silver/Gold) |
| **BI** | Apache Superset 6.0.0 | Dashboard analitik dari Gold layer (OBT) |
| **Monitoring** | Prometheus + Grafana | Monitoring WAL Lag, resource, dll |

## Tech Stack

| Teknologi | Kegunaan |
|-----------|----------|
| **Debezium (Kafka Connect)** | CDC engine — kelola replication slot, logical decoding, schema evolution |
| **PostgreSQL** | Source database transaksional E-Commerce (WAL logical replication) |
| **Redpanda** | Kafka-compatible message broker + Schema Registry (payload JSON) |
| **ClickHouse** | Data warehouse OLAP (Kafka Engine, Materialized Views, Query-time Views) |
| **Apache Superset** | BI dashboard dari Gold layer (One Big Table) |
| **Prometheus + Grafana** | Monitoring pipeline (WAL Lag, resource metrics) |
| **FastAPI (Python)** | Order service & traffic generator (Faker) |
| **Docker Compose** | Orchestrasi 13+ container |

## Dokumentasi

| Dokumen | Isi |
|---------|-----|
| [System Architecture](docs/system_architecture.md) | Diagram komponen & alur data |
| [Data Architecture](docs/data_architecture.md) | Medallion (Bronze → Silver → Gold) |
| [Data Dictionary](docs/data_dictionary_gold.md) | Skema & metrik tabel Gold (OBT) |
| [ERD Source](docs/erd_source.md) | Entity Relationship Diagram PostgreSQL |
| [ERD Warehouse](docs/erd_warehouse.md) | Entity Relationship Diagram ClickHouse (Bronze/Silver/Gold) |
| [ADR-001: Migrasi ke Debezium](docs/adr/ADR-001-migration-to-dbz.md) | Keputusan arsitektur migrasi CDC |
| [Legacy vs Debezium](docs/cdc_legacy_vs_debezium.md) | Perbandingan implementasi pglogrepl lama vs Debezium |
| [ADR-004: Pengujian Pipeline](adr/ADR-004-performance-reliability-testing.md) | Keputusan dan evaluasi framework eksperimen |
| [Runbook Eksperimen](docs/experiments/adr-004-e2e-runbook.md) | Skenario, pengukuran, rekonsiliasi & pemulihan |
| [Analisis Kapasitas](docs/experiments/capacity-analysis.md) | Hasil baseline, steady-state & spike |

---

## Cara Jalankan

**Prerequisites:** Docker & Docker Compose, buat file `.env` (lihat `.env.example`)

```bash
# 1. Start semua service
make docker-up

# 2. Inisialisasi komponen (urutan penting)
make init-redpanda          # Buat topic cdc-events
make init-db                # Buat tabel E-Commerce di PostgreSQL (users, products, orders, order_items)
make init-dbz               # Generate & register connector Debezium (dari template + env)
make init-clickhouse        # Bronze Layer (CDC queue) + Silver/Gold Layer (views + OBT)
make init-superset          # Init admin & import dashboard

# 3. Seed data & testing
make seed-db                # Seed data dummy E-Commerce (users & products)
make generate-traffic       # Bot transaksi E-Commerce otomatis (real-time testing)

# 4. Monitoring
make logs-tg                # Lihat log traffic generator
```

## Pengujian Performa & Resilience

Framework CLI pada `services/experiment-controller` mengatur traffic sintetis dan menjalankan eksperimen pada stack **dev**. Workload menggunakan YAML untuk target request, durasi, konkurensi, timeout, dan pola flash sale. Skenario mencakup gangguan Debezium, Redpanda, Go consumer, dan ClickHouse, serta pengamatan backlog dan pemulihan.

```bash
# Siapkan stack dev dan dependensi CLI
make docker-up-dev
python3 -m pip install -r services/experiment-controller/requirements.txt

# Jalankan baseline tanpa fault (pastikan generator dev lain berhenti)
python3 services/experiment-controller/cli.py --environment dev traffic stop
python3 services/experiment-controller/cli.py --environment dev experiment capacity --kind baseline --tps 25 --duration 300

# Contoh fault: hentikan consumer 30 detik, lalu pulihkan
python3 services/experiment-controller/cli.py --environment dev experiment consumer-recovery --duration 30
```

Gunakan fault **hanya pada stack non-production yang terisolasi**. Skenario mencoba memulihkan service setelah gangguan, kegagalan, atau interupsi; jika proses dibunuh paksa atau host mati, periksa dan pulihkan service secara manual. Jangan hapus data atau offset untuk eksperimen.

Hasil run disimpan di `services/experiment-controller/logs/`, termasuk konfigurasi, metrik yang tersedia, dan ringkasan recovery. Pemeriksaan order sumber terhadap ClickHouse tersedia melalui `experiment reconcile-orders`; cakupannya saat ini terbatas pada order `create`, belum seluruh event CDC. Lihat [daftar perintah](services/experiment-controller/COMMANDS.md), [kontrak metrik](docs/experiments/experiment-contract-template.md), dan [bukti eksperimen](docs/experiments/adr-004-e2e-evidence.md) untuk parameter, keterbatasan, dan langkah lengkap.

## Akses Service

| Service | URL | Keterangan |
|---------|-----|------------|
| **Order API** | http://localhost:8000/docs | FastAPI Swagger |
| **Debezium Connect** | http://localhost:8083 | REST API Kafka Connect (status connector) |
| **Redpanda Console** | http://localhost:8888 | Lihat stream pesan Debezium (JSON + Schema Registry) |
| **Redpanda Schema Registry** | http://localhost:8081 | Registry skema payload Debezium |
| **Superset** | http://localhost:8088 | Dashboard BI (admin / lihat .env) |
| **Grafana** | http://localhost:3000 | Monitoring pipeline — WAL Lag, CPU, memory, disk (lihat .env) |
| **Prometheus** | http://localhost:9090 | Metrics pipeline |

### Grafana — Monitoring Dashboard

Monitoring pipeline secara real-time mencakup:
- **WAL Lag** — delay antara PostgreSQL dan CDC
- **Resource Usage** — CPU, memory, disk dari Node Exporter
- **Postgres Metrics** — koneksi, replikasi, long running transactions

![Grafana — OS, WAL, Redpanda Metrics Monitoring](docs/screenshots/grafana-monitoring.png)

### Superset — BI Dashboard

Dashboard analitik E-Commerce dari Gold layer (One Big Table), siap untuk eksplorasi data real-time.

![Superset Dashboard — Sales Analytics](docs/screenshots/superset-dashboard.png)

## Validasi Real-time

Pipeline ini sudah divalidasi dengan cara:
1. **Hentikan connector Debezium** → event berhenti masuk, lag terlihat di Redpanda/Grafana
2. **Aktifkan lagi** → data catch up dan sinkron kembali
3. Data di ClickHouse selalu sinkron dengan PostgreSQL dalam hitungan detik

## Struktur Proyek

```
├── services/
│   ├── order-service/         # FastAPI + Data Generator (Faker)
│   ├── grafana/               # Grafana provisioning & dashboards
│   ├── prometheus/            # Prometheus config
│   └── postgres-source/       # Init SQL source
├── connectors/                # Konfigurasi connector Debezium (template + hasil generate)
├── scripts/
│   └── sql/                   # DDL PostgreSQL & ClickHouse (init_source, init_clickhouse)
├── deployments/docker/        # Docker Compose + konfigurasi
│   ├── docker-compose.yml
│   ├── Dockerfile.superset    # Superset 6.0.0 + ClickHouse driver
│   ├── clickhouse-users.xml
│   ├── init-superset.sh
│   └── dashboards/
├── docs/
│   ├── adr/                   # ADR-001: Migrasi ke Debezium
│   ├── cdc_legacy_vs_debezium.md
│   ├── screenshots/
│   └── ...
├── Makefile                   # Command utama
└── .env                       # Konfigurasi environment (jangan di-commit)
```

*Dibuat untuk keperluan belajar Data Engineering — Real-time Pipeline Journey.*
