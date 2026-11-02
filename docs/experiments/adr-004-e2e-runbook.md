# ADR-004: runbook eksperimen reliability end-to-end

## Tujuan dan batas

Amati aliran order PostgreSQL → Debezium → Redpanda → consumer → ClickHouse
pada keadaan normal dan ketika satu komponen dihentikan sementara. Rekonsiliasi
membandingkan ID order sumber yang masih ada pada jendela pengamatan dengan
baris `operation = 'c'` pada `orders_history`. Lebih dari satu baris create
untuk ID yang sama adalah **kandidat duplikasi**, bukan pembuktian identitas
setiap event CDC. Update, delete, order yang dihapus, tabel lain, transaksi yang
melewati batas snapshot, dan perubahan di luar jendela tidak tercakup.

## Kondisi awal dan keselamatan

- Pilih `--environment dev`; pastikan project `realtime-pipeline-dev` terisolasi
  dari production, termasuk database, broker, volume, network, dan traffic.
  Label Compose saja tidak membuktikan isolasi semua dependensi.
- PostgreSQL, order service, Debezium, Redpanda, consumer, dan ClickHouse hidup;
  connector dan task `RUNNING`, broker dan consumer sehat, consumer group aktif.
- Siapkan schema, connector dan data produk; catat versi image, batas resource,
  konfigurasi, lag awal, jumlah generator aktif, dan kondisi awal service.
- Jalankan satu generator saja; contoh target 25 request/detik, flash sale mati.
  Pastikan transaksi sukses benar-benar masuk selama baseline dan gangguan.
- Pastikan ruang disk/WAL cukup dan tidak ada eksperimen lain berjalan. Perintah
  fault menghentikan service dev sungguhan. Jangan hapus data atau offset.
- Tetapkan ambang lulus/gagal dan batas waktu recovery sebelum menjalankan.
  Tidak ada angka SLO yang ditetapkan secara otomatis dalam runbook ini.

## Kontrak dan kriteria

Hipotesis bersama: setelah gangguan satu komponen dan pemulihannya, order yang
diamati di sumber pada jendela pengujian muncul sebagai create di sink.
Lulus untuk cakupan ini bila workload nonzero, fault terverifikasi, service dan
connector pulih, consumer mencapai target offset (jika tersedia), tidak ada ID
sumber yang belum ditemukan atau kandidat duplikasi create, dan metrik memenuhi
ambang yang dipilih. Jika metrik atau cakupan tidak memadai, hasil **tidak dapat
 disimpulkan**; jangan perlakukan data tak tersedia sebagai nol. Status
`completed` pada controller hanya menandakan alur kode selesai, bukan kelulusan
SLO atau bukti tidak ada kehilangan seluruh jenis event.

`orders-reconciliation.json` dibuat oleh `reconcile-orders`: untuk run fault,
jendelanya dari `fault_started_at` sampai `recovery_started_at`; untuk capacity,
dari awal traffic sampai ringkasan traffic. PostgreSQL dev harus menggunakan
UTC. Tunggu yang ditetapkan adalah batas menunggu sink; order yang belum terlihat
di akhir pengamatan dilaporkan sebagai missing, bukan dipastikan hilang permanen.

## Workload dan baseline normal

Dari root repo, pastikan image generator terbaru sudah dibangun; hentikan generator
dev lain terlebih dahulu. Jalankan baseline normal:

```bash
make docker-build-dev
python3 services/experiment-controller/cli.py --environment dev traffic stop
python3 services/experiment-controller/cli.py --environment dev experiment capacity --kind baseline --tps 25 --duration 300
```

Gunakan path JSON yang dicetak run, lalu rekonsiliasi:

```bash
python3 services/experiment-controller/cli.py --environment dev experiment reconcile-orders --run services/experiment-controller/logs/capacity/runs/<run-id>/capacity-result.json --wait 120
```

Ganti `<run-id>` dengan ID aktual. Catat attempted/success RPS, timeout/error,
p50/p95/p99, lag, CPU/memori, batas resource, dan hasil rekonsiliasi. `capacity`
menjalankan generator sementara dengan konfigurasi run; tidak otomatis menjamin
tidak ada generator lain di luar Compose.

## Skenario gangguan

Untuk setiap skenario, gunakan workload yang sama dan biarkan generator berjalan
selama baseline, gangguan, dan pemulihan. Misalnya, dengan durasi generator yang
cukup untuk seluruh eksperimen:

```bash
python3 services/experiment-controller/cli.py --environment dev traffic start --tps 25 --duration 1200 --no-flash-sale
```

Jalankan satu gangguan pada satu waktu:

```bash
python3 services/experiment-controller/cli.py --environment dev experiment debezium-recovery --duration 10 --baseline-duration 10 --sample-interval 5 --recovery-timeout 180
python3 services/experiment-controller/cli.py --environment dev experiment redpanda-interruption --duration 10 --baseline-duration 10 --sample-interval 5 --recovery-timeout 180
python3 services/experiment-controller/cli.py --environment dev experiment consumer-recovery --duration 10 --baseline-duration 10 --sample-interval 5 --recovery-timeout 180
python3 services/experiment-controller/cli.py --environment dev experiment clickhouse-recovery --duration 10 --baseline-duration 10 --sample-interval 5 --recovery-timeout 180
```

Untuk masing-masing JSON yang selesai, jalankan rekonsiliasi dengan path yang
dicetak controller:

```bash
python3 services/experiment-controller/cli.py --environment dev experiment reconcile-orders --run <path-result-json> --wait 120
```

| Gangguan | Hipotesis dan pengukuran tambahan |
| --- | --- |
| Debezium | WAL tertinggal lalu posisi confirmed mengejar target; catat status connector dan waktu recovery. |
| Redpanda | Publisher/consumer terdampak, broker dan connector pulih; catat log, lag yang tersedia dan recovery. Lag saat broker mati tidak tersedia, bukan nol. |
| Consumer | Offset log bertambah ketika consumer berhenti; catat pertumbuhan backlog dan waktu mencapai offset target. |
| ClickHouse | Consumer gagal menulis saat sink mati; catat log error, restart container, lag, waktu drain dan rekonsiliasi. Jumlah baris log bukan jumlah retry yang pasti. |

## Cleanup dan recovery

Controller mencoba memulai service kembali setelah fault, error, Ctrl+C, atau
SIGTERM. SIGKILL, mesin mati, dan kegagalan Docker tidak menjamin cleanup. Jika
perlu, pulihkan service terkait dan cek status/connector secara manual:

```bash
python3 services/experiment-controller/cli.py --environment dev debezium resume
python3 services/experiment-controller/cli.py --environment dev redpanda resume
python3 services/experiment-controller/cli.py --environment dev consumer resume
python3 services/experiment-controller/cli.py --environment dev clickhouse resume
python3 services/experiment-controller/cli.py --environment dev traffic stop
```

Pastikan service yang awalnya hidup kembali sehat, connector dan group aktif,
lag turun, dan data sink teramati. Jangan otomatis memulai service yang memang
sebelumnya dihentikan. Simpan JSON run, hasil rekonsiliasi, log, konfigurasi,
dan catatan jika ada failure; log hasil di folder `logs/` tidak dilacak Git.

## Bukti pelaksanaan

Lihat [ringkasan bukti](adr-004-e2e-evidence.md) untuk hasil dan keterbatasan
run yang telah dilakukan. Identitas run bukan pengganti artefak JSON mentah.
