# Skenario 1: Baseline tanpa gangguan

**Referensi:** [kontrak dan kriteria](experiment-contract-template.md) · [ADR-004](../../adr/ADR-004-performance-reliability-testing.md)

## Tujuan dan kondisi awal

Hipotesis: pada target 25 request/detik selama 300 detik, pipeline dev dapat memproses workload tanpa fault dengan hasil memenuhi kriteria awal pada kontrak. Kondisi awal: project `realtime-pipeline-dev` terisolasi dari production; PostgreSQL, Debezium, Redpanda, consumer, ClickHouse dan order service siap; connector dan task RUNNING; schema dan produk tersedia; satu generator saja; catat lag awal dan batas resource. Tidak perlu mengosongkan database.

## Parameter dan langkah

- Target 25 request/detik; maksimum 50 request bersamaan; timeout 5 detik.
- Durasi 300 detik; flash sale nonaktif; tunggu hingga 120 detik untuk rekonsiliasi.
- Ambang yang ditetapkan sebelum run baru: lihat tabel kriteria baseline pada kontrak.

Dari root repository, build image terbaru dan pastikan generator dev lain berhenti:

```bash
make docker-build-dev
python3 services/experiment-controller/cli.py --environment dev traffic stop
python3 services/experiment-controller/cli.py --environment dev experiment capacity --kind baseline --tps 25 --duration 300
```

Gunakan path JSON yang dicetak perintah untuk rekonsiliasi:

```bash
python3 services/experiment-controller/cli.py --environment dev experiment reconcile-orders --run <path-capacity-result.json> --wait 120
```

Catat run ID, konfigurasi, hasil traffic, lag, resource dan rekonsiliasi. Periksa kondisi service setelah run. Kegagalan/interupsi perlu cleanup generator; periksa dan hentikan hanya generator yang dibuat untuk run tersebut.

## Hasil

Run baru `20261102T095013Z-67ac701a` selesai pada 2026-11-02 setelah kriteria awal ditetapkan. Evidence: [hasil run](evidence/adr-004/baseline-das2-run.json) dan [rekonsiliasi](evidence/adr-004/baseline-das2-orders-reconciliation.json).

| Metrik | Hasil | Kriteria | Penilaian |
| --- | --- | --- | --- |
| Attempted RPS | 24,890 | ≥ 24 | Lulus |
| Successful TPS | 24,890; 7.467 dari 7.467 attempted | ≥ 24 dan ≥ 99% | Lulus |
| Gagal / timeout | 0 / 0 | ≤ 1% gabungan | Lulus |
| p50 / p95 / p99 | 14,470 / 28,248 / 58,228 ms | p95 ≤ 100 ms | Lulus |
| Consumer lag | 19 sampel valid; maksimum 67, akhir 58 offset | maksimum ≤ 500, akhir ≤ 200 | Lulus |
| Order create | 7.467 ID diperiksa, 0 belum ditemukan, 0 kandidat duplikasi | ≥ 1 ID, missing 0, kandidat duplikasi 0 | Lulus untuk cakupan order create |
| Source-to-sink lag dan semua jenis event | Belum terukur | Belum dapat diverifikasi | Tidak dapat disimpulkan |

**Hasil: lulus terbatas** untuk metrik yang tersedia dan cakupan order create, bukan lulus menyeluruh CDC atau SLO produksi. Durasi aktual 300,005 detik. Source-to-sink latency dan kehilangan/duplikasi seluruh jenis event tetap belum dapat disimpulkan. Run lama `20261102T085401Z-fd88088c` adalah referensi, bukan bukti lulus terhadap ambang baru.
