## ADR-004: Pengujian Performa dan Keandalan Pipeline dengan Skrip Eksperimen

* **Status:** Proposed
* **Date:** 2026-11-01

## Konteks

Proyek ini digunakan untuk memperluas pengalaman dalam arsitektur data, menganalisis performa sistem, dan melengkapi *capacity planning*. Untuk itu, diperlukan cara menjalankan berbagai skenario beban dan gangguan serta mengamati perilaku pipeline selama dan setelahnya.

*Requirement engineering* mencakup identifikasi, dokumentasi, dan pemeliharaan kebutuhan perangkat lunak, termasuk analisis kebutuhan pengguna dan penyusunan spesifikasi. *Capacity planning* menentukan sumber daya yang diperlukan untuk memenuhi kebutuhan sistem.

### Kondisi saat ini

Data sintetis dari Faker dikirim ke PostgreSQL, kemudian diteruskan ke ClickHouse melalui Debezium, Redpanda, dan Go Consumer.

Traffic generator (`services/order-service/data_generator.py`) mengirim request ke endpoint `/simulate/order` pada order service. Order service memilih user dan produk, membuat order beserta item, dan memperbarui stok produk di PostgreSQL. Dengan demikian, satu request order dapat menghasilkan beberapa perubahan CDC; jumlah request tidak sama dengan jumlah event CDC.

Pola traffic saat ini ditentukan dalam kode: setiap siklus memilih 2–5 request, kecuali setiap siklus ke-30 yang memilih 30–50 request untuk flash sale. Request dijalankan secara bersamaan dengan maksimal 50 worker dan timeout lima detik. Generator menunggu semua request dalam siklus selesai, lalu menunggu satu detik sebelum siklus berikutnya. Karena waktu pemrosesan ditambahkan ke jeda tersebut, angka yang dipilih merupakan jumlah request per siklus, bukan jaminan TPS aktual; flash sale juga terjadi setiap 30 siklus, bukan selalu setiap 30 detik. Generator berjalan terus sampai dihentikan.

Perubahan pola tersebut saat ini memerlukan perubahan kode generator. Service `traffic-generator` dapat dihentikan dan dijalankan melalui target Makefile, sedangkan `make generate-traffic` menjalankan proses generator tambahan di container order service. Selain mengatur beban, diperlukan cara untuk menyimulasikan gangguan pada komponen pipeline dan mengamati pemulihannya.

### Tujuan

Menyediakan eksperimen yang dapat dijalankan secara fleksibel dan berulang untuk menguji performa dan keandalan sistem. Hasilnya diharapkan membantu mengidentifikasi masalah, mengoptimalkan kinerja, dan memahami kemampuan sistem dalam berbagai kondisi operasional.

## Alternatif yang Dipertimbangkan

### 1. Dashboard interaktif

Dashboard menyediakan kontrol untuk jumlah data dan status traffic generator, serta pause/resume dan simulasi error pada PostgreSQL, Debezium, Redpanda, Go Consumer, ClickHouse, dan Superset. Fitur dapat dikembangkan secara bertahap.

Dashboard memudahkan pengguna menjalankan berbagai skenario, tetapi membutuhkan waktu pengembangan dan pemeliharaan tambahan serta pemahaman terhadap setiap komponen yang dikendalikan.

### 2. Skrip atau alat command-line

Skrip menyediakan perintah untuk mengatur beban dan menyimulasikan gangguan pada komponen. Parameter eksperimen dapat ditentukan saat menjalankan perintah, sehingga skenario dapat diulang.

Pendekatan ini lebih cepat dan sederhana untuk dikembangkan daripada dashboard. Kekurangannya, pengguna perlu memahami perintah yang tersedia dan antarmukanya kurang mudah digunakan dibandingkan dashboard.

### 3. Mempertahankan kondisi saat ini

Tidak ada usaha tambahan untuk mengubah alur yang ada. Namun, pengujian tetap kurang fleksibel dan mungkin tidak memenuhi kebutuhan skenario yang lebih kompleks.

## Keputusan

Memilih **alternatif 2: menggunakan skrip atau alat command-line** sebagai langkah pertama. Pengembangan dilakukan secara bertahap, dimulai dari kontrol beban dan skenario gangguan yang dibutuhkan untuk pengujian pipeline.

Ubah traffic generator agar membaca konfigurasi eksternal, misalnya YAML, INI, atau format lain yang sesuai. Konfigurasi ini memungkinkan pengaturan target TPS atau laju request dasar, pola dan interval flash sale, besaran serta durasi spike, durasi eksperimen, batas konkurensi, timeout, dan pengaturan lain yang diperlukan tanpa mengubah kode. Format dan nilai default ditentukan saat implementasi; perilaku lama perlu dipertimbangkan agar penggunaan yang sudah ada tetap jelas.

Implementasi perlu mendefinisikan apakah target TPS mengacu pada request yang dicoba atau transaksi yang berhasil, mengatur penjadwalan berdasarkan waktu, dan mencatat laju aktual beserta keberhasilan dan kegagalan. Target adalah beban yang diupayakan, bukan jaminan throughput sistem. Konfigurasi harus divalidasi, dan cara menyediakan file ke service Docker serta kapan perubahan konfigurasi berlaku perlu didokumentasikan. Hindari menjalankan beberapa generator tanpa sengaja saat menggunakan service dan target Makefile.

Setiap skenario didokumentasikan dengan kondisi awal, hipotesis, langkah eksperimen, metrik yang diamati, dan kriteria lulus. Parameter dan hasil pengujian dicatat agar eksperimen dapat diulang dan dibandingkan.

Dashboard dapat dipertimbangkan kemudian jika jumlah skenario, frekuensi penggunaan, atau kebutuhan antarmuka terpadu membenarkan biaya pengembangannya. Skrip yang dibuat dapat menjadi dasar kontrol untuk dashboard tersebut.

## Skenario yang Direncanakan

- Mengatur laju dasar, spike flash sale, dan durasi traffic melalui file konfigurasi eksternal, serta menghidupkan atau mematikan generator untuk menguji kemampuan sistem hilir menangani beban.
- Menjeda dan melanjutkan Debezium untuk mengamati CDC lag dan pemulihan.
- Menjeda dan melanjutkan Redpanda untuk mengamati dampak pada broker dan consumer.
- Menjeda dan melanjutkan Go Consumer untuk mengamati penumpukan pesan dan pemulihan backlog.
- Menjeda dan melanjutkan ClickHouse untuk mengamati respons saat data warehouse tidak dapat menerima data.
- Mengembangkan skenario untuk PostgreSQL, Superset, dan komponen lain sesuai kebutuhan, termasuk simulasi error.

Sebagai contoh, eksperimen consumer dapat menjeda consumer selama periode tertentu saat traffic berjalan. Hipotesisnya, backlog bertambah selama jeda dan berkurang setelah consumer dilanjutkan. Metrik yang diamati meliputi consumer lag, waktu pemulihan, throughput, serta event yang hilang atau terduplikasi. Kriteria lulus dan parameter ditentukan sebelum eksperimen dijalankan.
 
## Konsekuensi

**Positif**

- Kontrol eksperimen dapat tersedia lebih cepat dan digunakan kembali.
- Skenario dan parameternya dapat didokumentasikan serta dijalankan berulang.
- Hasil pengujian dapat mendukung analisis performa, keandalan, dan capacity planning.

**Negatif**

- Pengoperasian memerlukan pengetahuan command-line.
- Skrip dan definisi eksperimen tetap perlu dipelihara ketika komponen berubah.
- Pengumpulan dan analisis hasil tetap memerlukan metrik serta kriteria yang sesuai.

## Penilaian

Kebutuhan pengujian ini layak dipenuhi karena mendukung pembelajaran dan membantu memahami perilaku pipeline ketika beban meningkat atau komponen terganggu. Skrip merupakan titik awal yang sesuai dengan kebutuhan saat ini: eksperimen dapat dirancang dan diulang tanpa terlebih dahulu membangun dashboard. Kebutuhan dashboard dapat dinilai kembali setelah pengalaman menjalankan beberapa skenario.

## Retrospektif dan tinjauan status (2026-11-02)

Implementasi awal menyediakan konfigurasi traffic berbasis YAML, controller dev
untuk beban dan gangguan komponen, ringkasan traffic, serta hasil run dan contoh
analisis kapasitas. Eksperimen recovery telah dijalankan pada stack dev;
[runbook end-to-end](../docs/experiments/adr-004-e2e-runbook.md) menjelaskan
cara mengulang skenario dan batas rekonsiliasi order.

Keputusan menggunakan skrip/CLI tetap relevan; dashboard belum diperlukan untuk
membuktikan skenario awal. Namun, bukti end-to-end yang tersedia terbatas pada
order create yang masih ada di sumber. Identitas event CDC yang stabil untuk
membuktikan kehilangan dan duplikasi seluruh update/delete belum tersedia;
metrik delay end-to-end serta penghitungan retry yang pasti juga belum lengkap.

**Tinjauan status:** tetap **Proposed** sampai cakupan pengukuran, ambang
kelulusan, dan bukti yang diperlukan disepakati dan diverifikasi. Status ini
bukan penilaian bahwa semua skenario gagal atau bahwa keputusan CLI ditolak.
