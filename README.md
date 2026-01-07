# Implementasi Pipeline Big Data ETL dan ELT: Evaluasi Penerbangan Maskapai AS Berdasarkan Cuaca

![Python](https://img.shields.io/badge/Python-3.10%2B-blue?style=for-the-badge&logo=python)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-15%2B-336791?style=for-the-badge&logo=postgresql)
![Power BI](https://img.shields.io/badge/Power_BI-Dashboard-F2C811?style=for-the-badge&logo=powerbi)

**Disusun Oleh Tim Mahasiswa S1 Teknik Komputer Telkom University:**
* **Muhammad Dafi Fathurrahman** (1103220186) - *Pipeline ETL*
* **Ryan Darell Adyatma** (1103223007) - *Pipeline ELT*
* **Chandra Aulia Haswangga** (1103223163) - *Dashboard Analitik*

---

## 📌 Deskripsi Studi Kasus

Sektor penerbangan Amerika Serikat menghadapi tantangan efisiensi operasional yang kompleks akibat variabilitas cuaca[cite: 10]. [cite_start]Proyek ini mengembangkan solusi *Big Data Analytics* untuk menganalisis korelasi antara kondisi cuaca dan kinerja penerbangan (2019–2023) pada 10 kota dengan lalu lintas terpadat, termasuk Chicago, Atlanta, dan New York.

Masalah utama yang diselesaikan adalah integrasi data penerbangan yang bersifat transaksional dengan data cuaca *time-series* dari berbagai sumber yang memiliki struktur berbeda [cite: 109-111]. Sistem ini membandingkan implementasi pipeline **ETL** dan **ELT** untuk menghasilkan *Data Warehouse* yang mendukung analisis metrik seperti *On-Time Performance* (OTP) dan tingkat pembatalan (*Cancellation Rate*)

---

## 🏗️ Arsitektur Sistem

Proyek ini menerapkan dua desain arsitektur pipeline untuk tujuan komparasi:

### 1. Pipeline ETL (Extract-Transform-Load)
Pemrosesan data dilakukan secara lokal menggunakan Python sebelum dimuat ke penyimpanan.
* **Extract:** Mengambil data CSV dari Kaggle dan data cuaca via Open-Meteo API.
* **Transform (Lokal):** Menggunakan library **Pandas**. Proses mencakup *cleaning* (imputasi nilai NaN, penghapusan baris inkonsisten), standardisasi kolom, *encoding* kategorikal, dan *feature engineering* (seperti menghitung selisih suhu dan tekanan udara antara bandara asal dan tujuan) [cite: 256-263].
* **Load:** Data yang sudah bersih dimuat ke tabel fakta dan dimensi di database PostgreSQL.

### 2. Pipeline ELT (Extract-Load-Transform)
Pemrosesan data dilakukan sepenuhnya di dalam *Data Warehouse*.
* **Extract & Load:** Memuat data mentah (*raw*) langsung ke schema `raw` di PostgreSQL/Data Lake tanpa pra-pemrosesan[cite: 268].
* **Transform (In-Database):** Menggunakan **SQL**. Data diproses bertahap dari layer `raw` -> `stg` (staging) -> `gold` (final). Transformasi meliputi *cleaning*, *joining* kompleks antar tabel, serta validasi kualitas data secara terpusat di database.

**Visualisasi Akhir:**
Data dari warehouse (schema `gold`) dihubungkan ke **Microsoft Power BI** untuk visualisasi dashboard interaktif.

---

## ⚖️ Perbedaan ETL dan ELT yang Digunakan

Berdasarkan implementasi pada studi kasus ini, berikut perbandingan performa kedua pendekatan:

| Aspek | ETL (Extract-Transform-Load) | ELT (Extract-Load-Transform) |
| :--- | :--- | :--- |
| **Pusat Pemrosesan** | **Python (Pandas)**. Beban komputasi pada mesin lokal/notebook. | **Database (PostgreSQL)**. Beban komputasi ditangani *engine* database. |
| **Kompleksitas** | Relatif simpel untuk *prototyping* dan eksplorasi data cepat. Kode Python lebih mudah dibaca untuk logika prosedural. | Membutuhkan desain layer schema (*raw, stg, gold*) dan query SQL (DDL/DML) yang kompleks, namun pipeline lebih terstruktur. |
| **Skalabilitas** | **Terbatas** pada RAM lokal. Performansi menurun drastis saat memproses *join* dataset besar. | **Tinggi**. Operasi *set-based* SQL sangat efisien untuk *join* besar dan agregasi data dalam warehouse. |
| **Fleksibilitas** | Perubahan logika *cleaning* mengharuskan proses ulang dari awal (extract ulang). | Data mentah tetap tersimpan di layer `raw`. Transformasi ulang cukup dilakukan dari layer staging tanpa memuat ulang sumber data. |
| **Kesesuaian Kasus** | Kurang ideal untuk data besar dengan kebutuhan *join* data spasio-temporal yang berat. | **Lebih unggul** untuk studi kasus ini karena efisiensi integrasi data *flights* & *weather* yang dilakukan terpusat di warehouse. |

---
