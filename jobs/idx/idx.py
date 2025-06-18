# DEPRECATED - File ini sudah diganti dengan struktur baru
# Struktur baru:
# - _scrap.py: Main scraping logic
# - idx_2024.py, idx_2023.py, idx_2022.py, idx_2021.py: Individual year processors
# - dags/idx_2024.py, dags/idx_2023.py, dags/idx_2022.py, dags/idx_2021.py: Airflow DAGs

"""
File ini tidak digunakan lagi.
Silakan gunakan struktur baru yang konsisten dengan YFinance:

1. jobs/idx/_scrap.py - Main scraping function
2. jobs/idx/idx_YYYY.py - Individual year processors  
3. dags/idx_YYYY.py - Airflow DAGs for each year

Data akan disimpan di MongoDB dengan koleksi "idx" dan sub-koleksi per tahun.
"""

print("File idx.py sudah deprecated. Gunakan struktur baru: _scrap.py dan idx_YYYY.py")
