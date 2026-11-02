# Experiment controller CLI

CLI ini mengendalikan traffic generator, PostgreSQL source, Debezium, Redpanda, consumer, dan ClickHouse pada Compose project
`realtime-pipeline-dev`. Python 3.9+, Docker Compose dan PyYAML diperlukan:

```bash
python3 -m pip install -r services/experiment-controller/requirements.txt
python3 services/experiment-controller/cli.py --environment dev traffic config --tps 10 --duration 60
python3 services/experiment-controller/cli.py --environment dev traffic start config --tps 10 --duration 60
python3 services/experiment-controller/cli.py --environment dev traffic stop
python3 services/experiment-controller/cli.py --environment dev traffic restart
```

Jalankan stack dev dan bangun image traffic generator terlebih dahulu. `start`
membuat atau menjalankan container tanpa memulai dependensi; order service dan
PostgreSQL perlu tersedia. Override Compose dev memasang `traffic-config.dev.yaml`
ke dalam container dan menonaktifkan restart otomatis agar eksperimen berdurasi
terbatas tidak berulang. Perubahan Compose berlaku saat container dibuat ulang.

Gunakan `traffic config --help` untuk melihat pengaturan konkurensi, timeout, flash sale,
dan `--unlimited`. Konfigurasi diubah pada file dev; restart generator untuk
menerapkannya. Perubahan saat generator berjalan baru berlaku pada start berikutnya.
CLI ini belum menyediakan run manifest atau deteksi run ganda.

`traffic start` dan `traffic restart` juga menerima opsi konfigurasi secara langsung,
dengan atau tanpa kata `config`. Saat `start` disertai opsi, container dibuat ulang
agar konfigurasi baru terbaca; `traffic config` hanya menyimpan konfigurasi.

## PostgreSQL source

```bash
python3 services/experiment-controller/cli.py --environment dev postgres stop
python3 services/experiment-controller/cli.py --environment dev postgres resume
python3 services/experiment-controller/cli.py --environment dev postgres start
python3 services/experiment-controller/cli.py --environment dev postgres stop --duration 30
```

`stop` tanpa durasi membiarkan database berhenti sampai `start` atau `resume`
dijalankan. `start` dan `resume` hanya menjalankan container yang sudah ada,
tanpa batas durasi. `stop --duration` mensyaratkan service sedang berjalan dan
mencoba memulihkannya setelah durasi berakhir, terjadi kegagalan, atau Ctrl+C.
Jika proses dibunuh paksa atau mesin mati, jalankan `postgres resume` secara manual.
Perintah hanya menargetkan Compose project dev; traffic generator tidak diubah.

## Debezium

Perintah berikut menargetkan service Debezium (Kafka Connect) pada project dev,
bukan connector individual:

```bash
python3 services/experiment-controller/cli.py --environment dev debezium status
python3 services/experiment-controller/cli.py --environment dev debezium stop
python3 services/experiment-controller/cli.py --environment dev debezium resume
python3 services/experiment-controller/cli.py --environment dev debezium start
python3 services/experiment-controller/cli.py --environment dev debezium restart
python3 services/experiment-controller/cli.py --environment dev debezium stop --duration 30
```

`stop` tanpa durasi membiarkan service berhenti sampai dijalankan kembali.
`start` dan `resume` menjalankan container yang sudah ada tanpa batas durasi;
`restart` me-restart service, dan `status` menampilkan status container, bukan
status connector atau lag. `stop --duration` (detik, harus lebih dari nol)
mensyaratkan service sedang berjalan dan mencoba memulihkannya setelah durasi,
kegagalan, atau Ctrl+C. Jika proses dibunuh paksa atau mesin mati, jalankan
`debezium resume` secara manual. Perintah ini tidak mengukur CDC lag atau throughput.

## Redpanda

```bash
python3 services/experiment-controller/cli.py --environment dev redpanda status
python3 services/experiment-controller/cli.py --environment dev redpanda stop
python3 services/experiment-controller/cli.py --environment dev redpanda start
python3 services/experiment-controller/cli.py --environment dev redpanda resume
python3 services/experiment-controller/cli.py --environment dev redpanda restart
python3 services/experiment-controller/cli.py --environment dev redpanda stop --duration 30
```

Perintah menargetkan service broker `redpanda` di Compose project dev, bukan
Redpanda Console. `stop` tanpa durasi membiarkan broker berhenti hingga `start`
atau `resume`. Keduanya menjalankan container yang sudah ada tanpa batas durasi.
`status` menampilkan status container, bukan lag atau throughput broker.

`stop --duration` menerima durasi positif dalam detik dan memerlukan broker
sedang berjalan. CLI mencoba menjalankannya kembali setelah durasi, kegagalan,
atau Ctrl+C. Jika proses dibunuh paksa atau mesin mati, pulihkan secara manual
dengan `redpanda resume`. Pemulihan menjalankan container; kesiapan broker dan
pemulihan service lain harus diperiksa terpisah.

## Go consumer

```bash
python3 services/experiment-controller/cli.py --environment dev consumer status
python3 services/experiment-controller/cli.py --environment dev consumer stop
python3 services/experiment-controller/cli.py --environment dev consumer start
python3 services/experiment-controller/cli.py --environment dev consumer resume
python3 services/experiment-controller/cli.py --environment dev consumer restart
python3 services/experiment-controller/cli.py --environment dev consumer stop --duration 30
```

Perintah menargetkan service `consumer` pada Compose project dev. `stop` tanpa
durasi membiarkan consumer berhenti hingga `start` atau `resume` dijalankan.
Keduanya menjalankan container yang sudah ada tanpa batas durasi. `status`
menampilkan status container, bukan consumer lag.

`stop --duration` menerima durasi positif dalam detik dan memerlukan consumer
sedang berjalan. CLI mencoba menjalankannya kembali setelah durasi, kegagalan,
atau Ctrl+C. Jika proses dibunuh paksa atau mesin mati, jalankan `consumer resume`
secara manual. Pemulihan menjalankan container; consumer lag dan waktu pemulihan
belum diukur oleh CLI.

## ClickHouse warehouse

```bash
python3 services/experiment-controller/cli.py --environment dev clickhouse status
python3 services/experiment-controller/cli.py --environment dev clickhouse stop
python3 services/experiment-controller/cli.py --environment dev clickhouse start
python3 services/experiment-controller/cli.py --environment dev clickhouse resume
python3 services/experiment-controller/cli.py --environment dev clickhouse restart
python3 services/experiment-controller/cli.py --environment dev clickhouse stop --duration 30
```

Perintah menargetkan service `clickhouse` pada Compose project dev. `stop` tanpa
durasi membiarkan warehouse berhenti sampai `start` atau `resume` dijalankan.
Keduanya menjalankan container yang sudah ada tanpa batas durasi. `status`
menampilkan status container, bukan kesiapan query atau kondisi consumer.

`stop --duration` menerima durasi positif dalam detik dan memerlukan warehouse
sedang berjalan. CLI mencoba menjalankannya kembali setelah durasi, kegagalan,
atau Ctrl+C. Jika proses dibunuh paksa atau mesin mati, jalankan `clickhouse
resume` secara manual. Pemulihan menjalankan container; kesiapan warehouse dan
pemulihan consumer perlu diperiksa terpisah.
