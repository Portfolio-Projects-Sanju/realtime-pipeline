# Bukti eksperimen ADR-004: 2026-11-02

Environment: Compose `realtime-pipeline-dev` pada satu VPS. Baseline sebelumnya
menggunakan target 25 request/detik selama 300 detik. Empat fault dijalankan
berurutan pada 2026-11-02 dengan generator sementara terpisah, target 25
request/detik, flash sale mati, dan durasi fault 10 detik. Generator fault
berjalan 355,765 detik, mencatat 8.854 attempted dan 8.854 sukses, tanpa gagal
atau timeout; successful RPS seluruh rangkaian 24,887. Ini bukan pengukuran
RPS per jendela fault. Service awalnya hidup; generator dev biasa sudah berhenti.

| Skenario | Run ID | Observasi controller | Order sumber di jendela | Belum ditemukan | Kandidat duplikasi create |
| --- | --- | --- | ---: | ---: | ---: |
| Normal | `20261102T085401Z-fd88088c` | Baseline selesai, 24,904 sukses RPS selama 300 detik | 7.472 | 0 | 0 |
| Debezium | `20261102T092400Z-5f453ffd` | Stop diverifikasi; start berhasil; target WAL terkonfirmasi dalam 94,418 detik setelah start | 445 | 0 | 0 |
| Redpanda | `20261102T092613Z-55ff7ae5` | Stop diverifikasi; start berhasil; kondisi recovery controller terpenuhi dalam 69,771 detik | 404 | 0 | 0 |
| Consumer | `20261102T092809Z-68a30f45` | Stop diverifikasi; backlog log-end bertambah 1.160 offset; target tercapai dalam 0,367 detik setelah start | 406 | 0 | 0 |
| ClickHouse | `20261102T092850Z-48455e9c` | Stop diverifikasi; start berhasil; consumer mencapai target dalam 5,820 detik setelah start | 366 | 0 | 0 |

## Perbandingan PostgreSQL dan ClickHouse

Tabel berikut membandingkan **ID order yang sama dalam cakupan setiap run**.
Kolom ClickHouse bukan jumlah semua baris di warehouse: itu adalah jumlah ID
order sumber yang ditemukan setidaknya sekali sebagai event `create` di
`orders_history`. Nilainya dihitung dari jumlah ID sumber dikurangi ID yang
belum ditemukan dalam hasil rekonsiliasi.

| Skenario | ID order di PostgreSQL yang diperiksa | ID yang ditemukan di ClickHouse | ID belum ditemukan saat pemeriksaan | ID dengan create lebih dari satu |
| --- | ---: | ---: | ---: | ---: |
| Normal | 7.472 | 7.472 | 0 | 0 |
| Debezium | 445 | 445 | 0 | 0 |
| Redpanda | 404 | 404 | 0 | 0 |
| Consumer | 406 | 406 | 0 | 0 |
| ClickHouse | 366 | 366 | 0 | 0 |

Untuk contoh Debezium: 445 ID sumber diperiksa, 445 ditemukan di ClickHouse,
sehingga selisih ID yang belum ditemukan adalah 0. Tidak ada ID yang memiliki
lebih dari satu baris `create` dalam pemeriksaan itu. Daftar ID yang belum
ditemukan dan jumlah create per ID yang terindikasi duplikat tersedia pada
file `*-orders-reconciliation.json` masing-masing skenario; pada run ini kedua
hasil tersebut kosong.

**Arti hasil:** `belum ditemukan` adalah data sumber yang tidak terlihat pada
saat pemeriksaan setelah masa tunggu; hasil ini tidak langsung membuktikan data
hilang permanen. `create` lebih dari satu merupakan kandidat duplikasi, bukan
bukti pasti untuk semua event CDC. Jumlah seluruh tabel PostgreSQL dan ClickHouse
tidak dibandingkan: ClickHouse menyimpan history, sehingga satu ID dapat
memiliki beberapa perubahan sah dan jumlah baris total memang bisa berbeda.

Setiap hasil run dan hasil rekonsiliasi yang dipakai dalam tabel disalin ke
[`evidence/adr-004/`](evidence/adr-004/); ringkasan traffic fault juga disimpan
di sana. Log lengkap dari controller tetap di folder lokal `logs/` masing-masing
run; file tersebut tidak dilacak Git. Hasil consumer dan ClickHouse di JSON
run menggunakan snapshot ID yang berbeda dari rekonsiliasi berbasis jendela
di tabel, sehingga jumlah order dapat berbeda.

Setelah cleanup, ClickHouse, consumer, Debezium, dan Redpanda kembali `Up`;
consumer dan Redpanda `healthy`; connector dan task `RUNNING`; group consumer
`Stable` dengan lag teramati 0. Generator sementara dihentikan. Ini adalah
snapshot setelah run, bukan jaminan kesehatan berikutnya.

Kesimpulan terbatas: order yang masih ada di PostgreSQL pada jendela yang
diamati ditemukan sebagai create di ClickHouse tanpa kandidat duplikasi create.
Ini tidak membuktikan seluruh event CDC, update, delete, tabel lain, atau
transaksi yang melewati batas jendela bebas kehilangan/duplikasi. Tidak ada
ambang SLO yang disepakati sebelum run, sehingga hasil ini bukan pernyataan
lulus SLO. Durasi recovery mencakup mekanisme berbeda dan tidak boleh disamakan
langsung sebagai latency end-to-end. Lihat [runbook](adr-004-e2e-runbook.md).
