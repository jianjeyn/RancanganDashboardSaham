# IDX Data Pipeline

Struktur baru untuk ETL data IDX yang konsisten dengan YFinance pipeline.

## Struktur File

### Jobs Directory (`jobs/idx/`)

1. **`_scrap.py`** - Main scraping logic
   - Fungsi utama untuk scraping data IDX
   - Menggunakan Selenium untuk web scraping
   - Memproses file XBRL yang diunduh
   - Menyimpan data ke MongoDB

2. **`idx_2024.py`** - Processor untuk data tahun 2024
3. **`idx_2023.py`** - Processor untuk data tahun 2023  
4. **`idx_2022.py`** - Processor untuk data tahun 2022
5. **`idx_2021.py`** - Processor untuk data tahun 2021

### DAGs Directory (`dags/`)

1. **`idx_2024.py`** - Airflow DAG untuk tahun 2024
2. **`idx_2023.py`** - Airflow DAG untuk tahun 2023
3. **`idx_2022.py`** - Airflow DAG untuk tahun 2022
4. **`idx_2021.py`** - Airflow DAG untuk tahun 2021

## Konfigurasi

### Environment Variables
File `.env` berisi:
```
MONGODB_URI='mongodb+srv://...'
MONGODB_DB='T3'
```

### Schedule
- Semua DAG dijadwalkan weekly pada hari Minggu jam 21:00 (`0 21 * * 0`)
- Tidak ada catch-up untuk menghindari overload

## MongoDB Collections

Data disimpan dalam koleksi `idx` dengan struktur:
```json
{
  "emitten": "AALI",
  "year": 2024,
  "Revenue": 1000000000,
  "GrossProfit": 500000000,
  "NetProfit": 200000000,
  // ... financial fields lainnya
}
```

## Financial Fields yang Di-extract

- Revenue (Pendapatan)
- GrossProfit (Laba Kotor)
- OperatingProfit (Laba Operasi) 
- NetProfit (Laba Bersih)
- Cash (Kas)
- TotalAssets (Total Aset)
- ShortTermBorrowing (Hutang Jangka Pendek)
- LongTermBorrowing (Hutang Jangka Panjang)
- TotalEquity (Total Ekuitas)
- CashFromOperating (Kas dari Operasi)
- CashFromInvesting (Kas dari Investasi)
- CashFromFinancing (Kas dari Pendanaan)

## Dependencies

Tambahan di `requirements.txt`:
- selenium (untuk web scraping)
- xmltodict (untuk parsing XBRL)

## Usage

1. **Manual Run**: `python jobs/idx/idx_2024.py`
2. **Via Airflow**: DAG akan berjalan otomatis sesuai schedule
3. **Testing**: Batasan 5 file per tahun untuk testing

## Notes

- File `idx.py` lama sudah deprecated
- Struktur baru konsisten dengan YFinance pipeline
- Data tersimpan per tahun untuk analisis yang lebih mudah
- Menggunakan headless Chrome untuk scraping
