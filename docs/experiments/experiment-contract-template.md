# Kontrak eksperimen, metrik, dan kriteria penerimaan

**ADR:** [ADR-004](../../adr/ADR-004-performance-reliability-testing.md)
**Task:** DAS-2 / ADR-004 Task 1/9
**Status:** ambang awal untuk eksperimen dev, ditetapkan 2026-11-02 sebelum baseline baru. Nilai ini adalah estimasi berdasarkan run sebelumnya, bukan SLO produksi atau kapasitas maksimum.

## Kondisi awal dan workload

Pipeline: PostgreSQL → Debezium → Redpanda → Go consumer → ClickHouse. Gunakan hanya project non-production `realtime-pipeline-dev` yang telah diverifikasi terisolasi, termasuk database, broker, volume, network, dan endpoint. Pastikan schema dan data produk tersedia, connector dan task RUNNING, broker serta consumer sehat, sink dapat diakses, tidak ada fault, dan hanya satu generator aktif. Catat versi image, batas resource, timestamp, dan lag awal. Jangan mengosongkan data atau mengubah offset untuk eksperimen ini.

Baseline memakai target **25 request/detik**, durasi **300 detik**, maksimum 50 request bersamaan, timeout 5 detik, dan flash sale nonaktif. Setelah traffic berakhir, beri waktu sampai 120 detik untuk rekonsiliasi. Spike dan recovery adalah skenario terpisah dengan konfigurasi dan waktu pengamatan yang dicatat.

| Konfigurasi YAML | Tipe / satuan | Makna |
| --- | --- | --- |
| `request.base_rate_per_second` | angka positif, request/detik | Target laju request yang dicoba. |
| `request.max_concurrency` | integer positif | Maksimum request bersamaan. |
| `request.timeout_seconds` | angka positif, detik | Timeout request. |
| `flash_sale.enabled` | boolean | Aktifkan pola spike. |
| `flash_sale.interval_seconds` | angka positif, detik | Interval antarmulai spike. |
| `flash_sale.duration_seconds` | angka positif, detik | Lama spike; tidak melebihi interval. |
| `flash_sale.rate_per_second` | angka positif, request/detik | Target selama spike menggantikan laju dasar. |
| `experiment.duration_seconds` | angka positif atau null, detik | Durasi traffic; null berjalan sampai dihentikan. |

Penjadwalan generator berbasis waktu; target tidak menjamin request terkirim atau sukses. Satu order menghasilkan beberapa perubahan CDC, sehingga RPS order tidak sama dengan event CDC per detik.

## Definisi dan metode

| Metrik | Definisi, metode, dan batas |
| --- | --- |
| Attempted RPS | Jumlah request yang benar-benar dicoba dibagi durasi aktual generator, termasuk waktu menunggu request terakhir selesai. Target yang terlewat tidak dihitung sebagai attempted. |
| Successful TPS | Respons HTTP 2xx dibagi durasi aktual; ini proksi order sukses, bukan verifikasi independen commit untuk request timeout. Timeout dicatat terpisah karena hasil transaksinya tidak pasti. |
| Latency request | Waktu dari mulai request sampai respons/error. p50/p95/p99 dihitung nearest-rank dalam ms atas seluruh request selesai, termasuk gagal dan timeout; tidak sama dengan latency CDC. |
| Consumer lag | Jumlah offset tertinggal yang dilaporkan `rpk group describe` per partisi; sampel saat tersedia, bukan jumlah order. Data tak tersedia bukan nol. |
| Source-to-sink lag | Selisih saat suatu event sumber terjadi dan event yang sama tersedia di sink. **Belum terinstrumentasi:** membutuhkan identitas event stabil, timestamp sumber dan waktu observasi sink yang dapat dibandingkan. WAL backlog byte dan consumer lag bukan penggantinya. |
| Missing/loss | `reconcile-orders` membandingkan ID order yang masih ada di PostgreSQL dalam jendela run dengan event create di `orders_history` setelah masa tunggu. ID belum ditemukan adalah missing pada waktu periksa, bukan bukti loss permanen. Update/delete, tabel lain, order yang dihapus, dan transaksi yang melewati batas snapshot belum dicakup. |
| Duplication | Untuk cakupan yang sama, lebih dari satu baris create per ID order dilaporkan sebagai kandidat duplikasi. Pembuktian duplikasi seluruh event butuh identitas stabil seperti topic/partition/offset; beberapa perubahan sah pada satu ID bukan otomatis duplikat. |
| Resource | Snapshot `docker stats` CPU dan memori per container beserta batas Docker dan image ID pada run; sampel bukan puncak pasti, dan CPU Docker dapat melebihi 100%. |
| Recovery | Untuk fault, catat waktu fault, pemulihan service, status connector, target offset/WAL, dan waktu sampai target tercapai. Kriteria berbeda dari latency end-to-end dan belum membuktikan seluruh CDC aman. |

## Kriteria awal baseline 25 RPS / 300 detik

Angka berikut **estimasi yang dipilih sebelum run baru**, bukan persyaratan produksi. Jendela metrik traffic adalah durasi aktual run; lag diambil dari sampel run; rekonsiliasi dilakukan hingga 120 detik setelah run. Tidak ada pengukuran berarti **tidak dapat disimpulkan**, bukan nol.

| Metrik | Lulus | Gagal / tidak dapat disimpulkan |
| --- | --- | --- |
| Attempted RPS | ≥ 24 request/detik, dengan attempted > 0 | < 24 gagal; counter/durasi tidak tersedia: tidak dapat disimpulkan. |
| Successful TPS | ≥ 24 respons sukses/detik dan ≥ 99% dari attempted | Di bawah salah satu batas gagal; tanpa counter: tidak dapat disimpulkan. |
| Error dan timeout | Total gagal + timeout ≤ 1% attempted | > 1% gagal; timeout tetap transaksi berstatus tidak pasti. |
| Request p95 | ≤ 100 ms | > 100 ms gagal; tanpa sampel latency: tidak dapat disimpulkan. p50/p99 tetap dilaporkan. |
| Consumer lag | Setidaknya dua sampel valid, maksimum ≤ 500 offset dan sampel akhir ≤ 200 offset | Melebihi batas gagal; sampel kurang/tidak tersedia: tidak dapat disimpulkan. |
| Order create missing | ≥ 1 ID sumber yang diperiksa dan 0 belum ditemukan dalam 120 detik | Ada yang belum ditemukan pada batas waktu: gagal untuk cakupan ini; tidak ada ID/data: tidak dapat disimpulkan. |
| Kandidat duplikasi create | 0 ID dengan lebih dari satu create dalam cakupan yang sama | Ada kandidat: gagal untuk cakupan ini dan perlu investigasi; tidak ada data: tidak dapat disimpulkan. |
| Source-to-sink lag dan seluruh event CDC | Tidak ada ambang kelulusan yang dapat diverifikasi saat ini | **Tidak dapat disimpulkan** sampai instrumentasi event dan timestamp tersedia. |

Keseluruhan **lulus terbatas** hanya jika semua metrik terukur di atas lulus; cakupan source-to-sink latency dan seluruh event CDC tetap tidak dapat disimpulkan. Hasil **gagal** jika salah satu metrik terukur gagal; jika tidak ada yang gagal tetapi data wajib tidak lengkap, **tidak dapat disimpulkan**. Tidak ada ambang recovery tunggal yang ditentukan untuk baseline tanpa fault; untuk setiap fault, pilih dan catat batas recovery sebelum run.

## Pelaksanaan dan keselamatan

Ikuti [baseline](01-baseline.md) dan [runbook end-to-end](adr-004-e2e-runbook.md). Baseline tidak menyuntikkan fault. Fault hanya pada environment non-production yang dipilih dan diverifikasi; catat kondisi awal, hentikan hanya target, pulihkan service yang sebelumnya berjalan, dan verifikasi connector, broker, consumer serta sink. Ctrl+C/SIGTERM mencoba cleanup; setelah SIGKILL atau mesin mati lakukan recovery manual. Jangan menganggap status container saja sebagai bukti aliran data atau konsistensi.
