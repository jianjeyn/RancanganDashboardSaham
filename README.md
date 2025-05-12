# Dashboard Saham - Big Data Project

**Mata Kuliah**: Big Data
**Judul Proyek**: Dashboard Saham Terintegrasi dengan Data Keuangan, Harga Saham, dan Berita

---

## Deskripsi Proyek

Proyek ini bertujuan untuk membangun sebuah **dashboard saham terintegrasi** yang menampilkan:

* Pergerakan harga saham harian dari berbagai emiten BEI
* Ringkasan laporan keuangan kuartalan dari situs resmi IDX
* Berita terkini yang diringkas otomatis dengan bantuan LLM (Large Language Model)

Dashboard dikembangkan untuk menampilkan **data secara real-time dan historis** dengan pipeline pengolahan data otomatis menggunakan **Apache Airflow**.

---

## Arsitektur Data & Pipeline

```
1. Scraping Data (IDX, yFinance, Berita) →  
2. Preprocessing (Parsing XML, Agregasi, Summarization) →  
3. Penyimpanan ke MongoDB →  
```

* Semua proses terotomatisasi menggunakan **Apache Airflow** sebagai orchestrator batch pipeline.


---

## Teknologi & Tools

| Komponen         | Teknologi                        |
| ---------------- | -------------------------------- |
| Orkestrasi       | Apache Airflow                   |
| Database         | MongoDB                          |
| Scraping         | Selenium                         |

---

## Cara Menjalankan

> Langkah singkat untuk menjalankan proyek (jika tersedia script)

```bash
# 1. Clone repo
git clone https://github.com/kelompok6/dashboard-saham.git
cd dashboard-saham

# 2. Aktifkan environment
conda activate bigdata-env

# 3. Jalankan Airflow
airflow scheduler
airflow webserver

# 4. Akses dashboard
localhost:8080
```
---

## Lisensi

Proyek ini dibuat untuk tujuan edukasi. Tidak untuk distribusi komersial tanpa izin.

