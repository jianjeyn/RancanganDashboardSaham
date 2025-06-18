import os
import time
import json
import shutil
import requests
import zipfile
import xmltodict
import pymongo
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from dotenv import load_dotenv

load_dotenv('/opt/airflow/jobs/.env')

MONGODB_URI = os.getenv("MONGODB_URI")
MONGODB_DB = os.getenv("MONGODB_DB")

client = pymongo.MongoClient(MONGODB_URI)
db = client[MONGODB_DB]

def extract_financials(xbrl_dict):
    """Extract financial data from XBRL dictionary"""
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

def scrape_idx_data(year, period="annual"):
    """Scrape IDX data for specific year and period
    
    Args:
        year (int): Year to scrape
        period (str): "annual" for yearly reports, "quarterly" for quarterly reports
    """
    print(f"Mengambil data IDX untuk tahun {year} periode {period}...")

    tickers = [
        'AADI',
        'AALI',
        'ABBA',
        'ABDA',
        'ABMM',
    ]

    download_folder = os.path.abspath("/opt/airflow/downloads")
    print(f"Folder download: {download_folder}")
    
    print(f"Mulai scraping data tahun {year} → folder download: {download_folder}")

    scraped_data = []
    
    # Konfigurasi Chrome WebDriver
    chrome_options = webdriver.ChromeOptions()
    prefs = {
        "download.default_directory": download_folder,
        "download.prompt_for_download": False,
        "safebrowsing.enabled": True,
    }
    chrome_options.add_experimental_option("prefs", prefs)
    chrome_options.add_argument("--headless=new")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--window-size=1920,1080")
    chrome_options.add_argument('--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36')
    chrome_options.add_argument("--disable-blink-features=AutomationControlled")
    chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
    chrome_options.add_experimental_option("useAutomationExtension", False)
    chrome_options.binary_location = "/usr/bin/chromium"
    service = Service("/usr/bin/chromedriver")
    driver = webdriver.Chrome(service=service, options=chrome_options)


    try:
        driver = webdriver.Chrome(options=chrome_options)
        
        # Set navigator.webdriver = false
        driver.execute_cdp_cmd("Page.addScriptToEvaluateOnNewDocument", {
            "source": """
            Object.defineProperty(navigator, 'webdriver', {
                get: () => undefined
            })
            """        })
        
        print("Membuka website IDX...")
        driver.get("https://www.idx.co.id/id/perusahaan-tercatat/laporan-keuangan-dan-tahunan")
        time.sleep(10)
        
        # Debug: Check if page loaded
        page_title = driver.title
        print(f"Page loaded - Title: {page_title}")
        
        # Debug: Look for any buttons first
        all_buttons = driver.find_elements(By.TAG_NAME, "button")
        print(f"Found {len(all_buttons)} buttons on page")
        
        # Cari filter button
        filter_button = None
        selectors = [
            "//button[contains(@class, 'btn-filter-input')]",
            "//button[contains(@class, 'filter')]", 
            "//button[contains(text(), 'Filter')]",
            "//button[contains(text(), 'filter')]",
            "//button[contains(text(), 'FILTER')]",
            "//div[contains(@class, 'filter')]//button",
            "//form//button[1]",
            "//button[1]"  # Try first button as fallback
        ]
        
        print("Mencari tombol filter...")
        for i, selector in enumerate(selectors):
            try:
                filter_elements = driver.find_elements(By.XPATH, selector)
                print(f"Selector {i+1} '{selector}': Found {len(filter_elements)} elements")
                if filter_elements:
                    filter_button = filter_elements[0]
                    print(f"Using filter button with text: '{filter_button.text}'")
                    break
            except Exception as e:
                print(f"Error with selector {i+1}: {e}")
                continue

        if not filter_button:
            print("DEBUG: Tidak menemukan tombol filter dengan selector yang dicoba.")
            # Optional: print all button texts for further debugging
            all_buttons = driver.find_elements(By.TAG_NAME, "button")
            print("DEBUG: Daftar semua tombol di halaman:")
            for idx, btn in enumerate(all_buttons):
                print(f"  [{idx}] Text: '{btn.text}' | Class: '{btn.get_attribute('class')}'")
        
        if filter_button:
            driver.execute_script("arguments[0].click();", filter_button)
            time.sleep(2)
            
            print(f"Mencari input tahun {year}...")
            # Cari input tahun
            year_selectors = [
                f"//input[@value='{year}']",
                f"//input[contains(@id, 'year') and @value='{year}']",
                f"//input[contains(@name, 'year') and @value='{year}']",
                f"//div[contains(@class, 'year')]//input[@value='{year}']"            ]
            
            year_input = None
            for selector in year_selectors:
                try:
                    year_elements = driver.find_elements(By.XPATH, selector)
                    if year_elements:
                        year_input = year_elements[0]
                        break
                except Exception as e:
                    continue
            
            if year_input:
                driver.execute_script("arguments[0].click();", year_input)
                
                # Pilih periode laporan berdasarkan parameter
                if period == "quarterly":
                    # Untuk quarterly, pilih triwulan 1
                    period_selectors = [
                        "//input[@value='Triwulan 1']",
                        "//input[contains(@id, 'quarterly') and contains(@value, '1')]",
                        "//input[contains(@name, 'periode') and contains(@value, 'Triwulan 1')]"
                    ]
                    
                    for selector in period_selectors:
                        try:
                            period_elements = driver.find_elements(By.XPATH, selector)
                            if period_elements:
                                driver.execute_script("arguments[0].click();", period_elements[0])
                                break
                        except Exception as e:
                            continue
                else:  # annual
                    # Pilih laporan tahunan
                    period_selectors = [
                        "//input[@value='Tahunan']",
                        "//input[contains(@id, 'annual')]",
                        "//input[contains(@name, 'periode') and contains(@value, 'Tahunan')]"
                    ]
                    
                    for selector in period_selectors:
                        try:
                            period_elements = driver.find_elements(By.XPATH, selector)
                            if period_elements:
                                driver.execute_script("arguments[0].click();", period_elements[0])
                                break
                        except Exception as e:
                            continue
                
                # Pilih laporan audit (saham)
                audit_selectors = [
                    "//input[@value='Saham']",
                    "//input[contains(@id, 'saham')]",
                    "//input[contains(@name, 'jenis') and contains(@value, 'Saham')]"
                ]
                
                for selector in audit_selectors:
                    try:
                        audit_elements = driver.find_elements(By.XPATH, selector)
                        if audit_elements:
                            driver.execute_script("arguments[0].click();", audit_elements[0])
                            break
                    except Exception as e:
                        continue
                
                # Apply filter
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
                            break
                    except Exception as e:
                        continue
                
                time.sleep(5)
                
                # Process download buttons (limited to first 5 for testing)
                download_buttons = driver.find_elements(By.XPATH, "//a[contains(@href, 'instance.zip')]")
                print(f"Jumlah tombol download ditemukan: {len(download_buttons)}")
                download_buttons = download_buttons[:5]
                
                for i, button in enumerate(download_buttons):
                    try:
                        print(f"Memproses tombol download {download_buttons}...")

                        print(f"Mengunduh file {i+1}/{len(download_buttons)} untuk tahun {year}...")

                        # Cari elemen box-title di atas tombol download
                        try:
                            container = button.find_element(By.XPATH, "./ancestor::div[contains(@class, 'container')]")
                            box_title = container.find_element(By.XPATH, ".//div[contains(@class, 'box-title')]")
                            ticker_span = box_title.find_element(By.XPATH, ".//span")
                            company_name = ticker_span.text.strip().upper()

                            print(f"✔ Ditemukan emiten: {company_name}")
                        except Exception as e:
                            print(f"❌ Gagal ambil nama emiten dari box-title: {e}")
                            continue

                        if company_name not in tickers:
                            print(f"Lewatkan {company_name} karena tidak ada di daftar tickers.")
                            continue
                        
                        # Download file
                        download_url = button.get_attribute('href')
                        if not download_url.startswith("http"):
                            download_url = f"https://www.idx.co.id{download_url}"

                        # Ekstrak ticker dari URL path
                        ticker_from_url = download_url.split("/")[-2].upper()

                        # Lanjutkan hanya jika ada di daftar tickers
                        if ticker_from_url not in tickers:
                            print(f"Lewatkan {ticker_from_url} karena tidak ada di daftar tickers.")
                            continue

                        print(f"✔ Ditemukan emiten dari URL: {ticker_from_url}")
                        print(f"🔗 Download URL: {download_url}")

                        # Cek nama file dari URL
                        file_name = download_url.split("/")[-1]
                        print(f"Nama file yang akan di-download: {file_name}")

                        response = requests.get(download_url)
                        # Debug: print nama file yang berhasil di-download
                        print(f"DEBUG: File berhasil di-download: {file_name}")

                        zip_path = os.path.join(download_folder, f"{ticker_from_url}_{year}.zip")
                        with open(zip_path, "wb") as f:
                            f.write(response.content)

                        print(f"📏 Ukuran file: {os.path.getsize(zip_path)} byte")
                        
                        # Find downloaded file
                        zip_files = [f for f in os.listdir(download_folder) if f.endswith('.zip')]
                        print(f"DEBUG: File ZIP yang ada di folder download: {zip_files}")
                        if zip_files:
                            latest_zip = max(zip_files, key=lambda x: os.path.getctime(os.path.join(download_folder, x)))
                            zip_path = os.path.join(download_folder, latest_zip)
                            
                            # Extract and process XBRL
                            with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                                extract_folder = os.path.join(download_folder, f'extract_{i}')
                                zip_ref.extractall(extract_folder)
                                
                                # Find XBRL files
                                for root, dirs, files in os.walk(extract_folder):
                                    for file in files:
                                        if file.endswith('.xbrl'):
                                            xbrl_path = os.path.join(root, file)
                                            
                                            try:
                                                with open(xbrl_path, 'r', encoding='utf-8') as xbrl_file:
                                                    xbrl_data = xmltodict.parse(xbrl_file.read())
                                                    financials = extract_financials(xbrl_data)
                                                    
                                                    record = {
                                                        "emitten": company_name,
                                                        "year": year,
                                                        "financials": financials
                                                    }
                                                    print(f"Record for {company_name} tahun {year}: {record}")
                                                    scraped_data.append(record)
                                                    break
                                            except Exception as e:
                                                print(f"Error processing XBRL {file}: {e}")
                                                continue
                              # Cleanup
                            os.remove(zip_path)
                            if os.path.exists(extract_folder):
                                shutil.rmtree(extract_folder)
                        
                        # Go back to main page
                        driver.get("https://www.idx.co.id/id/perusahaan-tercatat/laporan-keuangan-dan-tahunan")
                        time.sleep(2)                        
                    except Exception as e:
                        print(f"Error processing download {i+1}: {e}")
                        continue
                
    except Exception as e:
        print(f"ERROR: Gagal scraping data IDX untuk tahun {year}: {e}")
        # Print detailed error information
        import traceback
        print("Full error traceback:")
        traceback.print_exc()
        
        # Save page source for debugging if driver exists
        try:
            if 'driver' in locals():
                debug_file = f"/tmp/idx_error_debug_{year}_{period}.html"
                with open(debug_file, "w", encoding="utf-8") as f:
                    f.write(driver.page_source)
                print(f"Page source saved to {debug_file} for debugging")
        except:
            pass
    
    finally:
        if 'driver' in locals():
            driver.quit()
    
    # Save to MongoDB with year-specific collection
    try:
        if scraped_data:
            # Use different collection based on year
            collection_name = f"idx_{year}"
            if period == "quarterly":
                collection_name = f"idx_{year}_q1"
            
            collection = db[collection_name]
            
            # Transform data for MongoDB
            mongo_records = []
            for item in scraped_data:
                record = {
                    "emitten": item["emitten"],
                    "year": item["year"],
                    "period": period,
                    **item["financials"]
                }
                # Convert to float where possible
                for key, value in record.items():
                    if key not in ["emitten", "year", "period"] and value is not None:
                        try:
                            record[key] = float(value)
                        except (ValueError, TypeError):
                            record[key] = None
                mongo_records.append(record)
            
            collection.insert_many(mongo_records)
            print(f"Data IDX tahun {year} periode {period} berhasil disimpan ke MongoDB collection '{collection_name}'! ({len(mongo_records)} records)")
        else:
            print(f"Tidak ada data yang berhasil di-scrape untuk tahun {year} periode {period}")
    
    except Exception as e:
        print(f"ERROR: Gagal menyimpan data ke MongoDB: {e}")

    return scraped_data

if __name__ == "__main__":
    # For testing purposes
    scrape_idx_data(2024, "annual")
