# Analisis awal kapasitas pipeline

**Tanggal:** 2026-11-02 (UTC)  
**Status:** hasil awal; ambang kelulusan belum ditetapkan.

## Kondisi pengujian

Ketiga run memakai Compose project `realtime-pipeline-dev`. Generator, order
service, PostgreSQL, Debezium, Redpanda, consumer, dan ClickHouse berjalan pada
host yang sama. Batas CPU dan memori khusus container tidak tercatat pada
konfigurasi Docker yang diamati; host menampilkan memori sekitar 23,41 GiB.
Identitas image dan konfigurasi resource per service tersimpan pada JSON run.

Target adalah request yang dicoba, bukan jumlah transaksi sukses atau event CDC.
Laju sukses dihitung dari respons sukses dibagi durasi aktual. Latency adalah
waktu request generator ke order service; persentil memakai nearest-rank dan
mencakup request gagal/timeout. Consumer lag di bawah adalah jumlah offset yang
dilaporkan group pada titik sampling, bukan latency end-to-end.

Tidak ada ambang error, latency, atau stabilitas lag yang disepakati sebelum
run. Karena itu hasil tidak dinyatakan lulus atau gagal terhadap SLO.

## Hasil

| Run ID | Skenario | Durasi | Target RPS | Attempted / sukses RPS | p50 / p95 / p99 (ms) | Gagal / timeout | Lag awal / akhir (rentang sampel) |
| --- | --- | ---: | ---: | ---: | --- | --- | --- |
| `20261102T085401Z-fd88088c` | Baseline | 300 dtk | 25 | 24,904 / 24,904 | 13,864 / 25,189 / 38,120 | 0 / 0 | 0 / 65 (0–75) |
| `20261102T085917Z-b3c8ae19` | Steady | 600 dtk | 50 | 49,392 / 49,392 | 14,457 / 31,178 / 55,878 | 0 / 0 | 87 / 20 (0–144) |
| `20261102T090935Z-f8f7621a` | Spike | 120 dtk | 50 dasar, 100 spike | 55,075 / 55,075 | 15,183 / 124,061 / 258,757 | 0 / 0 | 0 / 0 (0–208) |

Run spike mengatur durasi spike 5 detik dengan interval 30 detik. RPS dan
persentil pada baris spike adalah ringkasan **seluruh run**, bukan pengukuran
khusus jendela spike. Karena itu, angka 55,075 RPS bukan bukti bahwa throughput
100 RPS tercapai saat spike.

Puncak CPU yang tersampel untuk order service adalah 23,14%, 45,40%, dan 87,26%
pada baseline, steady, dan spike. Untuk ClickHouse masing-masing 139,12%, 96,95%,
dan 96,96%. Persentase Docker dapat melebihi 100% jika lebih dari satu core
dipakai. Ini adalah puncak *sampel*, bukan puncak pasti selama seluruh run.

## Interpretasi

- **Steady yang teruji:** sekitar 49,4 request sukses/detik selama 10 menit,
  tanpa gagal atau timeout yang tercatat. Lag sampel berkisar 0–144 dan berakhir
  lebih rendah daripada sampel awal. Ini observasi untuk run tersebut, belum
  bukti kapasitas maksimum atau kestabilan jangka panjang.
- **Lonjakan:** p95 keseluruhan run spike sekitar 124 ms, dibandingkan 31 ms
  pada run steady. Throughput puncak dan latency khusus saat spike belum
  terukur terpisah; jangan menyamakannya dengan kapasitas berkelanjutan.
- **Bottleneck:** belum dapat dipastikan. CPU order service yang tersampel lebih
  tinggi saat spike dan ClickHouse juga menunjukkan penggunaan CPU yang berarti,
  tetapi sampel ini tidak membuktikan salah satunya sebagai pembatas sistem.
- **Recovery:** ada run recovery terpisah, tetapi belum dibandingkan secara
  numerik dengan run kapasitas ini karena kondisi dan jendelanya belum dipastikan
  setara.

## Tindak lanjut

Tetapkan ambang error, p95, tren lag, dan durasi steady sebelum run berikutnya.
Uji misalnya target 75 RPS dengan durasi dan kondisi yang sama, lalu ulangi
untuk menilai variasi. Tambahkan pengukuran per fase jika ingin menyatakan
throughput selama spike. Data sumber–sink, resource host keseluruhan, dan dampak
workload lain juga perlu diperiksa sebelum menyimpulkan kapasitas pipeline.
