import argparse
import csv
import fcntl
import os
import logging
import math
import random
import signal
import threading
import time
import requests
import concurrent.futures
from datetime import datetime, timezone
from pathlib import Path
from faker import Faker
from sqlalchemy.orm import Session
import yaml

# Import komponen database dari main.py
from main import User, Product, SessionLocal

fake = Faker()
logger = logging.getLogger(__name__)

# ==========================================
# 1. KATALOG PRODUK REALISTIS (ENTERPRISE)
# ==========================================
PRODUCT_CATALOG = {
    'Electronics': {
        'brands': ['Apple', 'Samsung', 'Sony', 'Asus', 'Dell'],
        'price_range': (200.0, 3000.0),
        'templates': ['{} Smart TV {} Inch', '{} Laptop Pro Gen {}', '{} Smartphone S{}', '{} Noise Cancelling Headphone V{}']
    },
    'Clothing': {
        'brands': ['Nike', 'Adidas', 'Uniqlo', 'H&M', 'Zara'],
        'price_range': (10.0, 200.0),
        'templates': ['{} Running Shoes Seri {}', '{} Casual T-Shirt Model {}', '{} Sport Jacket V{}', '{} Denim Fit-{}']
    },
    'Furniture': {
        'brands': ['IKEA', 'Informa', 'Ashley', 'Cellini'],
        'price_range': (50.0, 1500.0),
        'templates': ['{} Minimalist Sofa V{}', '{} Wooden Table Seri {}', '{} Ergonomic Chair {}', '{} Wardrobe Model {}']
    },
    'Beauty': {
        'brands': ['Loreal', 'Maybelline', 'Wardah', 'SK-II'],
        'price_range': (5.0, 150.0),
        'templates': ['{} Matte Lipstick V{}', '{} Anti-Aging Serum {}ml', '{} Foundation Shade {}', '{} Face Wash {}ml']
    }
}

def seed_db():
    """Fase 1: Membangun Data Warehouse Landing Zone dengan data yang masuk akal"""
    print("📦 [SEEDING] Mempersiapkan Database...")
    db: Session = SessionLocal()
    
    # Cek Idempotency (Jangan jalankan dua kali)
    if db.query(User).count() > 100:
        print("⚠️ [SEEDING] Data sudah ada. Melewati proses seeding untuk mencegah duplikasi.")
        db.close()
        return

    # 1. Generate Users
    print("👥 [SEEDING] Menyuntikkan 1000 Users acak...")
    users = [User(email=fake.unique.email(), full_name=fake.name()) for _ in range(1000)]
    db.add_all(users)

    # 2. Generate Products
    print("💻 [SEEDING] Menyuntikkan 500 Products realistis...")
    products = []
    for _ in range(500):
        category = random.choice(list(PRODUCT_CATALOG.keys()))
        meta = PRODUCT_CATALOG[category]
        brand = random.choice(meta['brands'])
        
        # Harga menggunakan Triangular Distribution (Cenderung ke arah harga murah/menengah)
        min_p, max_p = meta['price_range']
        mode_p = min_p + (max_p - min_p) * 0.2 # Puncak probabilitas ada di 20% harga bawah
        price = round(random.triangular(min_p, max_p, mode_p), 2)
        
        # Penamaan Produk Berbasis Template
        template = random.choice(meta['templates'])
        product_name = template.format(brand, random.randint(1, 99))
        
        products.append(Product(
            name=product_name,
            category=category,
            brand=brand,
            price=price,
            stock_quantity=random.randint(20, 500)
        ))
    
    db.add_all(products)
    db.commit()
    db.close()
    print("✅ [SEEDING] SELESAI: 1000 Users & 500 Products siap untuk simulasi transaksi!")


# ==========================================
# 2. SIMULATOR TRAFIK E-COMMERCE (MULTITHREADING)
# ==========================================
API_URL = "http://localhost:8000/simulate/order" # Gunakan localhost jika dari dalam container fastapi ke dirinya sendiri, atau "order-service" jika via docker network

def load_traffic_config():
    config_path = Path(__file__).resolve().with_name("traffic-config.yaml")
    with config_path.open(encoding="utf-8") as config_file:
        return yaml.safe_load(config_file)

class TrafficStats:
    """Thread-safe counters and latency totals for completed requests."""

    def __init__(self):
        self.lock = threading.Lock()
        self.attempted = self.success = self.failed = self.timeout = 0
        self.latencies = []
        self.started_at = datetime.now(timezone.utc)
        self.start_monotonic = time.monotonic()
        self.latency_total = 0.0
        self.latency_min = None
        self.latency_max = 0.0

    def record(self, outcome, latency):
        with self.lock:
            self.attempted += 1
            setattr(self, outcome, getattr(self, outcome) + 1)
            self.latencies.append(latency)
            self.latency_total += latency
            self.latency_min = latency if self.latency_min is None else min(self.latency_min, latency)
            self.latency_max = max(self.latency_max, latency)

    def write_csv(self, log_dir, config):
        """Save one experiment summary; latency is the mean in milliseconds."""
        log_dir = Path(log_dir)
        log_dir.mkdir(parents=True, exist_ok=True)
        now = datetime.now(timezone.utc)
        path = log_dir / "traffic.csv"
        average_ms = self.latency_total * 1000 / self.attempted if self.attempted else 0
        elapsed = time.monotonic() - self.start_monotonic
        ordered = sorted(self.latencies)
        def percentile(q):
            return round(ordered[math.ceil(q * len(ordered)) - 1] * 1000, 3) if ordered else None
        with path.open("a+", newline="", encoding="utf-8") as output:
            fcntl.flock(output, fcntl.LOCK_EX)
            try:
                columns = ["timestamp", "attempted", "sukses", "gagal", "timeout", "latency",
                           "base_rate_per_second", "duration_seconds", "flash_sale_enabled",
                           "flash_sale_rate_per_second", "flash_sale_interval_seconds",
                           "flash_sale_duration_seconds", "max_concurrency", "timeout_seconds",
                           "run_id", "started_at", "actual_duration_seconds", "attempted_rps", "success_rps",
                           "latency_p50_ms", "latency_p95_ms", "latency_p99_ms"]
                output.seek(0)
                reader = csv.DictReader(output)
                existing_columns = reader.fieldnames
                if existing_columns and existing_columns != columns:
                    if existing_columns not in (columns[:6], columns[:14]):
                        raise ValueError("Format CSV traffic tidak dikenal")
                    old_rows = list(reader)
                    output.seek(0)
                    output.truncate()
                    writer = csv.DictWriter(output, fieldnames=columns)
                    writer.writeheader()
                    writer.writerows(old_rows)
                elif not existing_columns:
                    csv.DictWriter(output, fieldnames=columns).writeheader()
                output.seek(0, os.SEEK_END)
                request = config["request"]
                flash = config["flash_sale"]
                writer = csv.DictWriter(output, fieldnames=columns)
                writer.writerow({"timestamp": now.isoformat(), "attempted": self.attempted,
                                 "sukses": self.success, "gagal": self.failed,
                                 "timeout": self.timeout, "latency": round(average_ms, 3),
                                 "base_rate_per_second": request["base_rate_per_second"],
                                 "duration_seconds": config["experiment"]["duration_seconds"],
                                 "flash_sale_enabled": flash["enabled"],
                                 "flash_sale_rate_per_second": flash.get("rate_per_second"),
                                 "flash_sale_interval_seconds": flash.get("interval_seconds"),
                                 "flash_sale_duration_seconds": flash.get("duration_seconds"),
                                 "max_concurrency": request["max_concurrency"],
                                 "timeout_seconds": request["timeout_seconds"],
                                 "run_id": os.environ.get("TRAFFIC_RUN_ID", ""),
                                 "started_at": self.started_at.isoformat(),
                                 "actual_duration_seconds": round(elapsed, 3),
                                 "attempted_rps": round(self.attempted / elapsed, 3) if elapsed > 0 else None,
                                 "success_rps": round(self.success / elapsed, 3) if elapsed > 0 else None,
                                 "latency_p50_ms": percentile(.50), "latency_p95_ms": percentile(.95),
                                 "latency_p99_ms": percentile(.99)})
                output.flush()
            finally:
                fcntl.flock(output, fcntl.LOCK_UN)
        return path


def send_transaction(worker_id: int, timeout_seconds: float, stats: TrafficStats):
    """Send one request and record its outcome and elapsed time."""
    started = time.monotonic()
    outcome = "failed"
    try:
        response = requests.post(API_URL, timeout=timeout_seconds)
        outcome = "success" if 200 <= response.status_code < 300 else "failed"
    except requests.exceptions.Timeout:
        outcome = "timeout"
    except requests.exceptions.RequestException as exc:
        logger.debug("[Worker-%s] Request gagal: %s", worker_id, exc)
    finally:
        stats.record(outcome, time.monotonic() - started)

def generate_traffic(config):
    """Fase 2: Menembakkan transaksi menggunakan Thread Pool dengan pola Gelombang"""
    logger.info("[TRAFFIC] Memulai simulator transaksi")
    request_config = config["request"]
    flash_sale = config["flash_sale"]
    duration = config["experiment"]["duration_seconds"]
    logger.info("Base load: %s request per detik", request_config["base_rate_per_second"])
    logger.info("Tekan Ctrl+C untuk menghentikan")
    
    stats = TrafficStats()
    # Gunakan thread pool untuk menembakkan request secara konkuren
    with concurrent.futures.ThreadPoolExecutor(max_workers=request_config["max_concurrency"]) as executor:
        started = time.monotonic()
        next_request = started
        pending = set()
        try:
            while True:
                now = time.monotonic()
                elapsed = now - started
                if duration is not None and elapsed >= duration:
                    logger.info("[TRAFFIC] Durasi eksperimen selesai")
                    break

                is_flash_sale = (flash_sale["enabled"] and
                                 elapsed >= flash_sale["interval_seconds"] and
                                 elapsed % flash_sale["interval_seconds"] < flash_sale["duration_seconds"])
                rate = flash_sale["rate_per_second"] if is_flash_sale else request_config["base_rate_per_second"]
                if now < next_request:
                    time.sleep(min(next_request - now, 0.1))
                    continue
                pending = {future for future in pending if not future.done()}
                if len(pending) < request_config["max_concurrency"]:
                    pending.add(executor.submit(send_transaction, len(pending), request_config["timeout_seconds"], stats))
                next_request = now + 1 / rate
                
        except KeyboardInterrupt:
            logger.info("[TRAFFIC] Simulator dihentikan")
    # Executor menunggu seluruh request yang sudah dikirim sebelum ringkasan dicatat.
    configured_log_dir = os.environ.get("TRAFFIC_LOG_DIR")
    log_dir = (Path(configured_log_dir) if configured_log_dir
               else Path(__file__).resolve().parent / "logs")
    stats.write_csv(log_dir, config)

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    parser = argparse.ArgumentParser(description="Script Manajemen Data & Simulator Trafik (Enterprise Grade)")
    parser.add_argument('--seed', action='store_true', help='Suntik Data Katalog & User')
    parser.add_argument('--traffic', action='store_true', help='Jalankan Robot Simulator Trafik')
    args = parser.parse_args()

    # Perbaiki URL jika dijalankan dari dalam Docker (sebagai service traffic-generator)
    if os.getenv("DOCKER_ENV") or os.path.exists("/.dockerenv"):
        API_URL = "http://order-service:8000/simulate/order"

    if args.seed:
        seed_db()
    elif args.traffic:
        traffic_config = load_traffic_config()
        def stop_traffic(signum, frame):
            raise KeyboardInterrupt
        signal.signal(signal.SIGTERM, stop_traffic)
        generate_traffic(traffic_config)
    else:
        parser.print_help()
