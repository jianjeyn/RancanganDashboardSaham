from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import os
import time
import json
import zipfile
import xmltodict
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from pymongo import MongoClient

# Default arguments untuk DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2025, 5, 12),
}

# Inisialisasi DAG
dag = DAG(
    'idx_stock_etl_pipeline',
    default_args=default_args,
    description='ETL pipeline untuk data saham IDX',
    schedule_interval=timedelta(days=1),  # Jalankan setiap hari
    catchup=False
)

# Fungsi Setup Environment
def setup_environment(**kwargs):
    """Membuat direktori untuk menyimpan file sementara"""
    download_folder = os.path.abspath("/tmp/downloads")
    os.makedirs(download_folder, exist_ok=True)
    return download_folder

# Fungsi Extract Financials
def extract_financials(xbrl_dict):
    tags = {
        "Revenue": ["idx-cor:SalesAndRevenue"],
        "GrossProfit": ["idx-cor:GrossProfit"],
        "OperatingProfit": ["idx-cor:ProfitLossFromContinuingOperations", "idx-cor:ProfitLoss"],
        "NetProfit": ["idx-cor:ProfitLoss", "idx-cor:ProfitLossAttributableToParentEntity"],
        "Cash": ["idx-cor:CashAndCashEquivalents"],
        "TotalAssets": ["idx-cor:Assets"],
        "ShortTermBorrowing": ["idx-cor:ShortTermBankLoans", "idx-cor:ShortTermLoans"],
        "LongTermBorrowing": ["idx-cor:LongTermBankLoans", "idx-cor:LongTermLoans"],
        "TotalEquity": ["idx-cor:Equity", "idx-cor:EquityAttributableToEquityOwnersOfParentEntity"],
        "CashFromOperating": ["idx-cor:NetCashFlowsReceivedFromUsedInOperatingActivities", "idx-cor:CashGeneratedFromUsedInOperations"],
        "CashFromInvesting": ["idx-cor:NetCashFlowsReceivedFromUsedInInvestingActivities"],
        "CashFromFinancing": ["idx-cor:NetCashFlowsReceivedFromUsedInFinancingActivities"]
    }

    result = {}

    for key, possible_tags in tags.items():
        value = None
        for tag in possible_tags:
            if tag in xbrl_dict:
                data = xbrl_dict[tag]
                # Kalau list, ambil object pertama
                if isinstance(data, list):
                    data = data[0]
                # Ambil nilai dari '#text'
                if isinstance(data, dict) and '#text' in data:
                    value = data['#text']
                elif isinstance(data, str):
                    value = data
                break  # tag sudah ketemu, lanjut ke key berikutnya
        result[key] = value

    return result

# Fungsi Scrape Data IDX
def scrape_idx_data(year, **kwargs):
    """Scrape data dari IDX website"""
    download_folder = kwargs['ti'].xcom_pull(task_ids='setup_environment')
    if not download_folder:
        download_folder = "/tmp/downloads"
        os.makedirs(download_folder, exist_ok=True)
    
    print(f"Using download folder: {download_folder}")
    
    # Inisialisasi variabel scraped_data di luar try-except
    scraped_data = []
    
    # Konfigurasi Chrome WebDriver dengan user agent yang lebih realistis
    chrome_options = webdriver.ChromeOptions()
    prefs = {
        "download.default_directory": download_folder,
        "download.prompt_for_download": False,
        "safebrowsing.enabled": True,
    }
    chrome_options.add_experimental_option("prefs", prefs)
    
    # Disarankan tidak menggunakan flag --headless untuk menghindari deteksi bot
    # chrome_options.add_argument("--headless")
    
    # Jika dalam container, tetap gunakan headless dengan tambahan flag
    chrome_options.add_argument("--headless=new")  # Versi headless baru
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    
    # Set window size yang lebih besar
    chrome_options.add_argument("--window-size=1920,1080")
    
    # Tambahkan user agent yang realistis
    chrome_options.add_argument('--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36')
    
    # Disable automation flags
    chrome_options.add_argument("--disable-blink-features=AutomationControlled")
    chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
    chrome_options.add_experimental_option("useAutomationExtension", False)
    
    # Untuk Chromium di Debian
    chrome_options.binary_location = "/usr/bin/chromium"
    
    print(f"Setting up Chrome options with binary: {chrome_options.binary_location}")
    print(f"Chrome options: {chrome_options.arguments}")
    
    try:
        print("Starting WebDriver")
        driver = webdriver.Chrome(options=chrome_options)
        
        # Set navigator.webdriver = false
        driver.execute_cdp_cmd("Page.addScriptToEvaluateOnNewDocument", {
            "source": """
            Object.defineProperty(navigator, 'webdriver', {
                get: () => undefined
            })
            """
        })
        
        print("WebDriver initialized successfully")
        
        # Buka halaman IDX
        print("Opening IDX website")
        driver.get("https://www.idx.co.id/id/perusahaan-tercatat/laporan-keuangan-dan-tahunan")
        
        # Tunggu lebih lama untuk memastikan halaman dimuat sepenuhnya
        time.sleep(10)
        
        # Debugging: Print page title and save screenshot
        print(f"Page title: {driver.title}")
        print(f"Current URL: {driver.current_url}")
        screenshot_path = os.path.join(download_folder, f"screenshot_{year}.png")
        driver.save_screenshot(screenshot_path)
        print(f"Screenshot saved to {screenshot_path}")
        
        # Tampilkan source HTML untuk debugging
        print("Page source length:", len(driver.page_source))
        with open(os.path.join(download_folder, f"page_source_{year}.html"), "w", encoding="utf-8") as f:
            f.write(driver.page_source)
        
        # Cek apakah ada Cloudflare protection
        if "Cloudflare" in driver.title:
            print("Terdeteksi Cloudflare protection, menunggu...")
            # Tunggu lebih lama untuk Cloudflare
            time.sleep(30)
            driver.save_screenshot(os.path.join(download_folder, f"after_wait_{year}.png"))
            print(f"After wait title: {driver.title}")
        
        # Cari filter button dengan lebih fleksibel, coba beberapa selector
        filter_button = None
        selectors = [
            "//button[contains(@class, 'btn-filter-input')]",
            "//button[contains(@class, 'filter')]",
            "//button[contains(text(), 'Filter')]",
            "//div[contains(@class, 'filter')]//button",
            "//form//button[1]"  # Coba button pertama dalam form
        ]
        
        for selector in selectors:
            try:
                filter_elements = driver.find_elements(By.XPATH, selector)
                if filter_elements:
                    filter_button = filter_elements[0]
                    print(f"Found filter button using selector: {selector}")
                    break
            except Exception as e:
                print(f"Failed to find filter with selector {selector}: {e}")
        
        if filter_button:
            driver.execute_script("arguments[0].click();", filter_button)
            time.sleep(2)
            
            # Coba temukan input tahun
            year_input = None
            year_selectors = [
                f"//input[@value='{year}']",
                f"//input[contains(@id, 'year') and @value='{year}']",
                f"//input[contains(@name, 'year') and @value='{year}']",
                f"//div[contains(@class, 'year')]//input[@value='{year}']"
            ]
            
            for selector in year_selectors:
                try:
                    year_elements = driver.find_elements(By.XPATH, selector)
                    if year_elements:
                        year_input = year_elements[0]
                        print(f"Found year input using selector: {selector}")
                        break
                except Exception as e:
                    print(f"Failed to find year with selector {selector}: {e}")
            
            if year_input:
                driver.execute_script("arguments[0].click();", year_input)
                
                # Pilih laporan tahunan (audit)
                audit_selectors = [
                    "//input[@value='audit']",
                    "//input[contains(@id, 'audit')]",
                    "//input[contains(@name, 'audit')]"
                ]
                
                for selector in audit_selectors:
                    try:
                        audit_elements = driver.find_elements(By.XPATH, selector)
                        if audit_elements:
                            driver.execute_script("arguments[0].click();", audit_elements[0])
                            print(f"Found audit input using selector: {selector}")
                            break
                    except Exception as e:
                        print(f"Failed to find audit with selector {selector}: {e}")
                
                # Cari tombol Apply
                apply_selectors = [
                    "//button[contains(text(), 'Terapkan')]",
                    "//button[contains(text(), 'Apply')]",
                    "//button[contains(@class, 'apply')]",
                    "//button[contains(@class, 'submit')]"
                ]
                
                for selector in apply_selectors:
                    try:
                        apply_elements = driver.find_elements(By.XPATH, selector)
                        if apply_elements:
                            driver.execute_script("arguments[0].click();", apply_elements[0])
                            print(f"Found apply button using selector: {selector}")
                            break
                    except Exception as e:
                        print(f"Failed to find apply button with selector {selector}: {e}")
                
                time.sleep(5)
                
                # Sisanya dari kode scraping...
                page = 1
                while True:
                    # Take screenshot of each page for debugging
                    driver.save_screenshot(os.path.join(download_folder, f"page_{year}_{page}.png"))
                    
                    # Ambil semua tombol download instance.zip
                    download_buttons = driver.find_elements(By.XPATH, "//a[contains(@href, 'instance.zip')]")
                    print(f"Found {len(download_buttons)} download buttons on page {page}")
                    
                    if len(download_buttons) == 0:
                        print("No download buttons found, breaking loop")
                        break
                    
                    # Lanjutkan sisa kode untuk mengunduh dan memproses file...
                    # ...
                    
                    page += 1
                    if page > 3:  # Batasi jumlah halaman untuk testing
                        break
                        
            else:
                print("Could not find year input")
        else:
            print("Could not find filter button")
            
    except Exception as e:
        print(f"Unexpected error during scraping: {e}")
        
    finally:
        # Always quit the driver
        if 'driver' in locals():
            print("Closing WebDriver")
            driver.quit()
    
    # Simpan data ke file JSON sementara (tetap jalan meskipun data kosong)
    output_file = os.path.join(download_folder, f"idx_data_{year}.json")
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(scraped_data, f, indent=4, ensure_ascii=False)
    
    print(f"Scraped data saved to {output_file} with {len(scraped_data)} records")
    return output_file

# Fungsi Transform Data
def transform_data(**kwargs):
    """Transform data dari semua tahun dan menggabungkannya"""
    download_folder = kwargs['ti'].xcom_pull(task_ids='setup_environment')
    years = [2024, 2023, 2022, 2021]
    
    all_data = []
    
    for year in years:
        json_file = kwargs['ti'].xcom_pull(task_ids=f'scrape_data_{year}')
        
        if json_file and os.path.exists(json_file):
            with open(json_file, 'r', encoding='utf-8') as f:
                year_data = json.load(f)
                all_data.extend(year_data)
    
    # Transformasi data menjadi format yang lebih sesuai untuk analisis
    transformed_data = []
    
    for item in all_data:
        emitten = item['emitten']
        year = item['year']
        financials = item['financials']
        
        # Convert financials to float where possible
        for key, value in financials.items():
            if value is not None:
                try:
                    financials[key] = float(value)
                except (ValueError, TypeError):
                    financials[key] = None
        
        record = {
            'emitten': emitten,
            'year': year,
            **financials
        }
        
        transformed_data.append(record)
    
    # Simpan hasil transformasi
    output_file = os.path.join(download_folder, "transformed_idx_data.json")
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(transformed_data, f, indent=4, ensure_ascii=False)
    
    return output_file

# Fungsi Load Data ke MongoDB
def load_to_mongodb(**kwargs):
    """Load data ke MongoDB"""
    transformed_data_file = kwargs['ti'].xcom_pull(task_ids='transform_data')
    
    with open(transformed_data_file, 'r', encoding='utf-8') as f:
        transformed_data = json.load(f)
    
    client = MongoClient("mongodb://mongodb:27017/")
    db = client["local"]
    collection = db["IDX"]
    
    # Hapus data lama jika ada
    collection.delete_many({})
    
    # Insert data baru
    collection.insert_many(transformed_data)
    
    return f"Loaded {len(transformed_data)} records to MongoDB"

# Define tasks
setup_task = PythonOperator(
    task_id='setup_environment',
    python_callable=setup_environment,
    dag=dag,
)

# Create scraping tasks for each year
scrape_tasks = {}
for year in [2024, 2023, 2022, 2021]:
    scrape_tasks[year] = PythonOperator(
        task_id=f'scrape_data_{year}',
        python_callable=scrape_idx_data,
        op_kwargs={'year': year},
        dag=dag,
    )

transform_task = PythonOperator(
    task_id='transform_data',
    python_callable=transform_data,
    dag=dag,
)

load_task = PythonOperator(
    task_id='load_to_mongodb',
    python_callable=load_to_mongodb,
    dag=dag,
)

# Set task dependencies to ensure that the scraping tasks run sequentially
setup_task >> scrape_tasks[2024] >> scrape_tasks[2023] >> scrape_tasks[2022] >> scrape_tasks[2021] >> transform_task >> load_task
