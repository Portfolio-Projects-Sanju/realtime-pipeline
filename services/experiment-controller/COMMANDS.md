# Experiment controller: daftar perintah

Jalankan dari root repository. Semua perintah menargetkan Compose project **dev**.

```bash
python3 -m pip install -r services/experiment-controller/requirements.txt
make docker-up-dev
python3 services/experiment-controller/cli.py --help
```

## Traffic generator

```bash
# Jalankan dengan konfigurasi yang tersimpan
python3 services/experiment-controller/cli.py --environment dev traffic start

# Atur konfigurasi sekaligus jalankan
python3 services/experiment-controller/cli.py --environment dev traffic start config --tps 10 --duration 60

# Konfigurasi saja, tanpa menjalankan generator
python3 services/experiment-controller/cli.py --environment dev traffic config --tps 10 --duration 60

# Jalankan tanpa batas durasi, sampai dihentikan
python3 services/experiment-controller/cli.py --environment dev traffic start --tps 10 --unlimited

# Jalankan dengan flash sale
python3 services/experiment-controller/cli.py --environment dev traffic start --tps 10 --duration 120 --flash-sale --flash-tps 100 --flash-interval 30 --flash-duration 5

# Matikan flash sale dan restart dengan konfigurasi baru
python3 services/experiment-controller/cli.py --environment dev traffic restart --no-flash-sale

# Stop atau restart dengan konfigurasi tersimpan
python3 services/experiment-controller/cli.py --environment dev traffic stop
python3 services/experiment-controller/cli.py --environment dev traffic restart

# Daftar opsi lengkap
python3 services/experiment-controller/cli.py --environment dev traffic config --help
```

| Parameter | Arti |
|---|---|
| `--tps 10` | Target request per detik; bukan jaminan request sukses. |
| `--duration 60` | Lama eksperimen dalam detik. |
| `--unlimited` | Berjalan sampai dihentikan; tidak dapat digabung dengan `--duration`. |
| `--max-concurrency 50` | Batas jumlah request bersamaan; bilangan bulat positif. |
| `--timeout 5` | Timeout request dalam detik. |
| `--flash-sale` / `--no-flash-sale` | Aktifkan / nonaktifkan flash sale. |
| `--flash-tps 100` | Target request per detik saat flash sale. |
| `--flash-interval 30` | Interval flash sale dalam detik. |
| `--flash-duration 5` | Durasi flash sale dalam detik, maksimal sama dengan interval. |

Nilai angka harus lebih dari nol. Opsi konfigurasi dapat digunakan pada `config`,
`start`, atau `restart`; kata `config` setelah `start`/`restart` opsional.
Konfigurasi disimpan di `services/order-service/traffic-config.dev.yaml`.
`config` saja tidak me-restart generator. `start` dengan opsi membuat ulang
container agar konfigurasi baru terbaca. Hasil traffic ditambahkan ke
`services/order-service/logs/traffic.csv` setelah generator selesai atau berhenti.

## PostgreSQL source

```bash
python3 services/experiment-controller/cli.py --environment dev postgres stop
python3 services/experiment-controller/cli.py --environment dev postgres stop --duration 30
python3 services/experiment-controller/cli.py --environment dev postgres start
python3 services/experiment-controller/cli.py --environment dev postgres resume
```

## Debezium

```bash
python3 services/experiment-controller/cli.py --environment dev debezium status
python3 services/experiment-controller/cli.py --environment dev debezium stop
python3 services/experiment-controller/cli.py --environment dev debezium stop --duration 30
python3 services/experiment-controller/cli.py --environment dev debezium start
python3 services/experiment-controller/cli.py --environment dev debezium resume
python3 services/experiment-controller/cli.py --environment dev debezium restart
```

## Redpanda

```bash
python3 services/experiment-controller/cli.py --environment dev redpanda status
python3 services/experiment-controller/cli.py --environment dev redpanda stop
python3 services/experiment-controller/cli.py --environment dev redpanda stop --duration 30
python3 services/experiment-controller/cli.py --environment dev redpanda start
python3 services/experiment-controller/cli.py --environment dev redpanda resume
python3 services/experiment-controller/cli.py --environment dev redpanda restart
```

## Go consumer

```bash
python3 services/experiment-controller/cli.py --environment dev consumer status
python3 services/experiment-controller/cli.py --environment dev consumer stop
python3 services/experiment-controller/cli.py --environment dev consumer stop --duration 30
python3 services/experiment-controller/cli.py --environment dev consumer start
python3 services/experiment-controller/cli.py --environment dev consumer resume
python3 services/experiment-controller/cli.py --environment dev consumer restart
```

## ClickHouse

```bash
python3 services/experiment-controller/cli.py --environment dev clickhouse status
python3 services/experiment-controller/cli.py --environment dev clickhouse stop
python3 services/experiment-controller/cli.py --environment dev clickhouse stop --duration 30
python3 services/experiment-controller/cli.py --environment dev clickhouse start
python3 services/experiment-controller/cli.py --environment dev clickhouse resume
python3 services/experiment-controller/cli.py --environment dev clickhouse restart
```

Untuk kelima service di atas, `stop --duration 30` menunggu 30 detik lalu
mencoba menjalankan kembali service; service harus sedang berjalan saat perintah
dimulai. Tanpa `--duration`, service tetap berhenti sampai `start` atau `resume`.
`start` dan `resume` menjalankan container yang sudah ada tanpa batas waktu.
Jika proses CLI dibunuh paksa atau mesin mati, jalankan `resume` secara manual.
`status` menunjukkan status container, bukan kesiapan service atau metrik lag.

## Eksperimen pemulihan Debezium

Pastikan stack dev terisolasi, connector dan slot yang dituju benar, serta traffic
berjalan stabil sepanjang baseline dan gangguan. Perintah ini benar-benar
menghentikan service Debezium dev:

```bash
python3 services/experiment-controller/cli.py --environment dev experiment debezium-recovery --duration 30 --baseline-duration 60 --recovery-timeout 300 --sample-interval 5 --connector ecommerce-postgres-source --slot ecommerce_debezium_slot
```

Baseline diambil sebelum gangguan pada run yang sama. Hasil dan status connector
tersimpan dalam `services/experiment-controller/logs/debezium-recovery/runs/<run-id>/debezium-recovery-result.json`.
`backlog_bytes` adalah selisih WAL PostgreSQL dan confirmed flush LSN, **bukan**
CDC delay dalam detik. `catchup_seconds` adalah waktu setelah perintah start
hingga connector RUNNING dan confirmed flush LSN mencapai posisi WAL saat fault
berakhir; ini tidak menjamin data sudah tiba di ClickHouse. Perbandingan maksimum
backlog adalah observasi, bukan kriteria lulus otomatis. Pertahankan beban yang
sama dan periksa status traffic generator; CLI tidak memverifikasi laju aktual.

Ctrl+C atau SIGTERM mencoba memulai kembali service. SIGKILL atau kegagalan mesin
tidak dapat ditangani; bila diperlukan, pulihkan secara manual:

```bash
python3 services/experiment-controller/cli.py --environment dev debezium resume
```

## Eksperimen gangguan Redpanda

Pastikan stack dev benar-benar terisolasi dari production, traffic berjalan stabil,
dan connector serta consumer group sudah sehat. Perintah ini menghentikan broker
dev secara nyata; jangan digunakan pada stack yang terhubung ke sistem produksi.

```bash
python3 services/experiment-controller/cli.py --environment dev experiment redpanda-interruption --duration 30 --baseline-duration 30 --recovery-timeout 300 --sample-interval 5 --connector ecommerce-postgres-source --slot ecommerce_debezium_slot
```

Hasil JSON dan log Debezium serta consumer disimpan di
`services/experiment-controller/logs/redpanda-interruption/runs/<run-id>/`.
Nama group otomatis dibaca persis dari container consumer; `--group` dapat digunakan untuk memilih nama lain. Baseline diambil sebelum gangguan. Sampel mencatat status connector, restart
consumer, WAL backlog, dan consumer-group lag saat tersedia. Saat broker mati,
consumer lag tidak dapat dibaca dan dicatat sebagai `null`, bukan nol. Jumlah
baris error/retry adalah pencarian kata pada log, bukan jumlah retry yang pasti.

Pemulihan mensyaratkan broker healthy, connector RUNNING, consumer healthy,
consumer group Stable, WAL mencapai posisi target saat fault berakhir, dan lag
tidak melebihi maksimum baseline. Ini tidak membuktikan data sampai ClickHouse.
CLI mencoba menjalankan broker kembali saat gagal, Ctrl+C, atau SIGTERM. Jika
proses dibunuh paksa atau mesin mati, pulihkan manual:

```bash
python3 services/experiment-controller/cli.py --environment dev redpanda resume
```
