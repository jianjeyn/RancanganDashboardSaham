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
from selenium.common.exceptions import StaleElementReferenceException

load_dotenv('/opt/airflow/jobs/.env')

MONGODB_URI = os.getenv("MONGODB_URI")
MONGODB_DB = os.getenv("MONGODB_DB")

client = pymongo.MongoClient(MONGODB_URI)
db = client[MONGODB_DB]

def debug_xbrl_structure(xbrl_dict, prefix="", max_depth=3, current_depth=0):
    """Debug function to inspect XBRL structure"""
    if current_depth >= max_depth:
        return
    
    if isinstance(xbrl_dict, dict):
        for key, value in xbrl_dict.items():
            if key.startswith('idx-cor:') and not key.endswith('Member'):
                if isinstance(value, (str, int, float)):
                    print(f"[DEBUG] {prefix}{key}: {value} (type: {type(value).__name__})")
                elif isinstance(value, list) and len(value) > 0:
                    if isinstance(value[0], dict) and '#text' in value[0]:
                        print(f"[DEBUG] {prefix}{key}: {value[0]['#text']} (from list[0]['#text'])")
                    elif isinstance(value[0], (str, int, float)):
                        print(f"[DEBUG] {prefix}{key}: {value[0]} (from list[0])")
                elif isinstance(value, dict) and '#text' in value:
                    print(f"[DEBUG] {prefix}{key}: {value['#text']} (from dict['#text'])")
            
            if isinstance(value, dict) and current_depth < max_depth - 1:
                debug_xbrl_structure(value, prefix + "  ", max_depth, current_depth + 1)
    elif isinstance(xbrl_dict, list) and len(xbrl_dict) > 0:
        debug_xbrl_structure(xbrl_dict[0], prefix, max_depth, current_depth)

def extract_company_name_from_xbrl(xbrl_dict):
    """Extract company name/ticker from XBRL entity identifier"""
    print("[DEBUG] Extracting company name from XBRL...")
    
    # Handle different root structures
    if 'xbrl' in xbrl_dict:
        xbrl_root = xbrl_dict['xbrl']
    elif 'xbrli:xbrl' in xbrl_dict:
        xbrl_root = xbrl_dict['xbrli:xbrl']
    else:
        xbrl_root = xbrl_dict
    
    # print(f"[DEBUG] XBRL root keys: {list(xbrl_root.keys()) if isinstance(xbrl_root, dict) else 'not a dict'}")
    
    # Look for context elements
    try:
        if 'context' in xbrl_root:
            contexts = xbrl_root['context']
            # print(f"[DEBUG] Found context, type: {type(contexts)}")
            
            # Handle both single context and list of contexts
            if isinstance(contexts, list):
                # Take the first context
                context = contexts[0]
            else:
                context = contexts
            
            # print(f"[DEBUG] Context keys: {list(context.keys()) if isinstance(context, dict) else 'not a dict'}")
            
            if isinstance(context, dict) and 'entity' in context:
                entity = context['entity']
                # print(f"[DEBUG] Entity keys: {list(entity.keys()) if isinstance(entity, dict) else 'not a dict'}")
                
                if isinstance(entity, dict) and 'identifier' in entity:
                    identifier = entity['identifier']
                    # print(f"[DEBUG] Identifier data: {identifier}, type: {type(identifier)}")
                    
                    if isinstance(identifier, dict):
                        # Check if it has the idx.co.id scheme
                        scheme = identifier.get('@scheme', '')
                        if 'idx.co.id' in scheme:
                            ticker = identifier.get('#text', '').strip().upper()
                            if ticker:
                                # print(f"[DEBUG] Found ticker from XBRL: {ticker}")
                                return ticker
                    elif isinstance(identifier, str):
                        # Sometimes identifier is directly a string
                        ticker = identifier.strip().upper()
                        if ticker:
                            # print(f"[DEBUG] Found ticker from XBRL (direct string): {ticker}")
                            return ticker
        
        print("[DEBUG] No entity identifier found in XBRL")
        return None
        
    except Exception as e:
        # print(f"[DEBUG] Error extracting company name: {e}")
        import traceback
        # print(f"[DEBUG] Full traceback: {traceback.format_exc()}")
        return None

def extract_financials(xbrl_dict):
    """Extract financial data from XBRL dictionary, with debug info for found/missing tags"""
    print("[DEBUG] Starting extract_financials...")
    
    if 'xbrli:xbrl' in xbrl_dict:
        xbrl_dict = xbrl_dict['xbrli:xbrl']
    elif 'xbrl' in xbrl_dict:
        xbrl_dict = xbrl_dict['xbrl']
    
    # print(f"[DEBUG] XBRL root keys: {list(xbrl_dict.keys())[:10]}...")  # Show first 10 keys
    
    # Debug the structure
    print("[DEBUG] Analyzing XBRL structure for idx-cor tags...")
    debug_xbrl_structure(xbrl_dict)
    
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
        found_tag = None
        for tag in possible_tags:
            # print(f"[DEBUG] Looking for tag: {tag}")
            if tag in xbrl_dict:
                data = xbrl_dict[tag]
                found_tag = tag
                # print(f"[DEBUG] Found tag {tag}, data type: {type(data)}, data: {str(data)[:200]}...")
                
                # Handle different data structures
                if isinstance(data, list):
                    # print(f"[DEBUG] Tag {tag} is a list with {len(data)} items")
                    # Look for the most recent data (usually the first or last item)
                    for item in data:
                        if isinstance(item, dict):
                            # Look for contextRef that contains CurrentYear
                            context_ref = item.get('@contextRef', '')
                            if 'CurrentYear' in context_ref:
                                if '#text' in item:
                                    value = item['#text']
                                    # print(f"[DEBUG] Found current year value: {value}")
                                    break
                        elif isinstance(item, (str, int, float)):
                            value = item
                            # print(f"[DEBUG] Found direct value from list: {value}")
                            break
                    
                    # If no CurrentYear found, take the first item
                    if value is None and data:
                        item = data[0]
                        if isinstance(item, dict) and '#text' in item:
                            value = item['#text']
                            # print(f"[DEBUG] Using first item value: {value}")
                        elif isinstance(item, (str, int, float)):
                            value = item
                            # print(f"[DEBUG] Using first item direct value: {value}")
                
                elif isinstance(data, dict):
                    # print(f"[DEBUG] Tag {tag} is a dict")
                    if '#text' in data:
                        value = data['#text']
                        # print(f"[DEBUG] Found #text value: {value}")
                    else:
                        # Look for contextRef that contains CurrentYear
                        for sub_key, sub_value in data.items():
                            if isinstance(sub_value, dict) and '#text' in sub_value:
                                context_ref = sub_value.get('@contextRef', '')
                                if 'CurrentYear' in context_ref:
                                    value = sub_value['#text']
                                    # print(f"[DEBUG] Found current year nested value: {value}")
                                    break
                        
                        # If still no value, try to get any #text value
                        if value is None:
                            for sub_value in data.values():
                                if isinstance(sub_value, dict) and '#text' in sub_value:
                                    value = sub_value['#text']
                                    # print(f"[DEBUG] Found any nested #text value: {value}")
                                    break
                
                elif isinstance(data, (str, int, float)):
                    value = data
                    # print(f"[DEBUG] Direct value: {value}")
                
                if value is not None:
                    break  # tag sudah ketemu, lanjut ke key berikutnya
        
        if found_tag:
            print(f"[extract_financials] Tag ditemukan untuk '{key}': {found_tag} → {value}")
        else:
            print(f"[extract_financials] Tag TIDAK ditemukan untuk '{key}' (tags dicoba: {possible_tags})")
        result[key] = value

    return result

def wait_until_file_downloaded(filepath, timeout=20):
    for _ in range(timeout):
        if os.path.exists(filepath) and not filepath.endswith(".crdownload"):
            return True
        time.sleep(1)
    return False

def wait_until_file_downloaded(filepath, timeout=20):
    for _ in range(timeout):
        if os.path.exists(filepath) and not filepath.endswith(".crdownload"):
            return True
        time.sleep(1)
    return False

def safe_click(driver, element, fallback=True):
    """Mencoba klik elemen dan menangani stale element exception."""
    try:
        driver.execute_script("arguments[0].scrollIntoView({behavior: 'smooth', block: 'center'});", element)
        time.sleep(1)
        driver.execute_script("arguments[0].click();", element)
        return True
    except StaleElementReferenceException as e:
        print(f"[safe_click] StaleElementReferenceException: {e}")
        if fallback:
            try:
                element.click()
                return True
            except Exception as fallback_err:
                print(f"[safe_click] Fallback click failed: {fallback_err}")
        return False
    except Exception as e:
        print(f"[safe_click] General click exception: {e}")
        return False

def scrape_idx_data(year, period):
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
                print(f"[PERIOD] Selecting period: {period}")
                
                if period == "quarterly":
                    # Untuk quarterly, pilih triwulan 1 (value="tw1", id="period0")
                    period_selectors = [
                        "//input[@value='tw1']",  # Exact value match
                        "//input[@id='period0']", # Exact ID match
                        "//input[@name='period'][@value='tw1']"  # Name + value match
                    ]
                    
                    for selector in period_selectors:
                        try:
                            period_elements = driver.find_elements(By.XPATH, selector)
                            print(f"[PERIOD] Selector '{selector}': Found {len(period_elements)} elements")
                            if period_elements:
                                driver.execute_script("arguments[0].click();", period_elements[0])
                                print(f"[PERIOD] ✓ Successfully selected Triwulan 1 (quarterly)")
                                break
                        except Exception as e:
                            print(f"[PERIOD] Error with selector '{selector}': {e}")
                            continue
                else:  # annual
                    # Pilih laporan tahunan (value="audit", id="period3")
                    period_selectors = [
                        "//input[@value='audit']",  # Exact value match 
                        "//input[@id='period3']",   # Exact ID match
                        "//input[@name='period'][@value='audit']"  # Name + value match
                    ]
                    
                    for selector in period_selectors:
                        try:
                            period_elements = driver.find_elements(By.XPATH, selector)
                            print(f"[PERIOD] Selector '{selector}': Found {len(period_elements)} elements")
                            if period_elements:
                                driver.execute_script("arguments[0].click();", period_elements[0])
                                print(f"[PERIOD] ✓ Successfully selected Tahunan (annual)")
                                break
                        except Exception as e:
                            print(f"[PERIOD] Error with selector '{selector}': {e}")
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
                
                time.sleep(5)                # Process download links (limited to first 5 for testing)
                download_links = driver.find_elements(By.XPATH, "//a[contains(@href, 'instance.zip')]")
                print(f"Jumlah link download ditemukan: {len(download_links)}")
                
                download_urls = []
                for link in download_links[:5]:  # Hanya ambil 5 teratas
                    try:
                        url = link.get_attribute('href')
                        if not url.startswith("http"):
                            url = f"https://www.idx.co.id{url}"
                        download_urls.append(url)
                    except Exception as e:
                        print(f"Error getting URL from link: {e}")
                        continue

                print(f"URLs yang akan didownload: {len(download_urls)}")
                  # Process each download URL by finding the element fresh each time to avoid stale reference
                for i in range(len(download_urls)):
                    try:
                        download_url = download_urls[i]
                        print(f"Memproses download {i+1}/{len(download_urls)} untuk tahun {year}...")
                        print(f"URL: {download_url}")

                        # Find the download element fresh each time to avoid stale reference
                        download_element = None
                        
                        # Try to find element by exact URL match
                        try:
                            relative_url = download_url.replace('https://www.idx.co.id', '')
                            download_element = driver.find_element(By.XPATH, f"//a[@href='{relative_url}']")
                            print(f"✓ Found element by exact URL match")
                        except:
                            # Fallback: find by partial URL match or index
                            try:
                                all_instance_links = driver.find_elements(By.XPATH, "//a[contains(@href, 'instance.zip')]")
                                if i < len(all_instance_links):
                                    download_element = all_instance_links[i]
                                    print(f"✓ Found element by index {i}")
                                else:
                                    print(f"✗ Element index {i} not found")
                                    continue
                            except Exception as e:
                                print(f"✗ Failed to find element: {e}")
                                continue
                        
                        if not download_element:
                            print(f"✗ Could not find download element for URL: {download_url}")
                            continue

                        # Skip checking company name from web scraping - kita akan ambil dari XBRL
                        # Langsung download file dulu                        
                        # Download file
                        print(f"Mengunduh file ke-{i+1} ({year})...")
                        existing_files = set(os.listdir(download_folder))
                        driver.execute_script("arguments[0].click();", download_element)
                        time.sleep(3)

                        # Cari file baru yang terunduh
                        new_files = set(os.listdir(download_folder)) - existing_files
                        if not new_files:
                            print("ERROR: Download gagal atau tidak ditemukan!")
                            continue

                        # Ambil nama file zip yang baru
                        file_name = None
                        for f in new_files:
                            if f.endswith('.zip'):
                                file_name = f
                                break
                        if not file_name:
                            print("ERROR: File ZIP tidak ditemukan di file baru!")
                            continue

                        # Tunggu file dengan nama asli selesai di-download
                        original_zip_path = os.path.join(download_folder, file_name)
                        print(f"DEBUG: Menunggu file di path: {original_zip_path}")
                        if wait_until_file_downloaded(original_zip_path):
                            print(f"✔ File berhasil diunduh: {file_name}")
                        else:
                            print(f"⛔ File tidak ditemukan: {file_name}")
                            continue
                        
                        # Find downloaded file
                        try:
                            zip_files = [f for f in os.listdir(download_folder) if f.endswith('.zip')]
                            print(f"DEBUG: File ZIP yang ada di folder download: {zip_files}")
                        except Exception as e:
                            print(f"[DEBUG] Error membaca folder download: {e}")
                            continue

                        if zip_files:
                            try:
                                latest_zip = max(zip_files, key=lambda x: os.path.getctime(os.path.join(download_folder, x)))
                                zip_path = os.path.join(download_folder, latest_zip)
                            except Exception as e:
                                print(f"[DEBUG] Error menentukan file ZIP terbaru: {e}")
                                continue
                            
                            # Extract and process XBRL
                            try:
                                with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                                    extract_folder = os.path.join(download_folder, f'extract_{i}')
                                    zip_ref.extractall(extract_folder)
                            except Exception as e:
                                print(f"[DEBUG] Error extract ZIP {zip_path}: {e}")
                                continue
                            
                            # Find XBRL files
                            print("Isi folder setelah extract:")
                            found_valid_xbrl = False
                            try:
                                for root, _, files in os.walk(extract_folder):
                                    for file in files:
                                        if file.endswith('.xbrl'):
                                            xbrl_path = os.path.join(root, file)
                                            print(f"DEBUG: Ditemukan file XBRL: {xbrl_path}")
                                            
                                            # Quick check what ticker is in this file
                                            quick_ticker = quick_check_xbrl_ticker(xbrl_path)
                                            if quick_ticker:
                                                print(f"🔍 QUICK CHECK: File contains ticker '{quick_ticker}'")
                                            else:
                                                print(f"🔍 QUICK CHECK: Could not determine ticker from file")
                                            
                                            try:
                                                with open(xbrl_path, 'r', encoding='utf-8') as xbrl_file:
                                                    xbrl_content = xbrl_file.read()
                                                    print("DEBUG XBRL raw content (first 1000 chars):", xbrl_content[:1000])
                                                    xbrl_data = xmltodict.parse(xbrl_content)
                                                    print("DEBUG XBRL root keys:", xbrl_data.keys())
                                                      # Extract company name dari XBRL
                                                    company_name = extract_company_name_from_xbrl(xbrl_data)
                                                    
                                                    if not company_name:
                                                        print("❌ Tidak bisa mendapatkan nama emiten dari XBRL, skip file ini")
                                                        continue
                                                    
                                                    # Validasi apakah ticker mengandung salah satu dari target tickers
                                                    matched_ticker = is_valid_ticker(company_name, tickers)
                                                    if not matched_ticker:
                                                        print(f"❌ Ticker '{company_name}' tidak mengandung target tickers {tickers}, skip file ini")
                                                        continue
                                                    
                                                    print(f"✔ Ditemukan emiten valid dari XBRL: '{company_name}' → matched ticker: '{matched_ticker}'")
                                                    
                                                    # Extract financial data
                                                    financials = extract_financials(xbrl_data)
                                                    
                                                    record = {
                                                        "emitten": matched_ticker,  # Use the matched ticker instead of full company_name
                                                        "year": year,
                                                        "financials": financials
                                                    }
                                                    print(f"Record for {matched_ticker} tahun {year}: {record}")
                                                    scraped_data.append(record)
                                                    found_valid_xbrl = True
                                                    break
                                            except Exception as e:
                                                print(f"[DEBUG] Error processing XBRL {file}: {e}")
                                                continue
                                    if found_valid_xbrl:
                                        break
                            except Exception as e:
                                print(f"[DEBUG] Error saat mencari file XBRL di folder extract: {e}")
                                continue
                            #   # Cleanup
                            # os.remove(zip_path)
                            # if os.path.exists(extract_folder):
                            #     shutil.rmtree(extract_folder)
                        
                        else:
                            print(f"[DEBUG] Tidak ada file ZIP ditemukan setelah download.")
                            continue                        # Go back to main page and re-apply filters for next iteration
                        try:
                            driver.get("https://www.idx.co.id/id/perusahaan-tercatat/laporan-keuangan-dan-tahunan")
                            time.sleep(2)
                            
                            # Re-apply filters to ensure we get fresh elements
                            if i < len(download_urls) - 1:  # Don't re-apply filters for the last iteration
                                reapply_filters(driver, year, period)
                                
                        except Exception as e:
                            print(f"[DEBUG] Error returning to main page or re-applying filters: {e}")
                            continue
                    except Exception as e:
                        print(f"Error processing download {i+1}: {e}")
                        print(traceback.format_exc())
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

def reapply_filters(driver, year, period="annual"):
    """Re-apply filters after navigating back to main page"""
    try:
        print(f"[FILTER] Re-applying filters for year {year}, period {period}")
        
        # Wait for page to load
        time.sleep(3)
        
        # Find and click filter button
        filter_button = None
        filter_selectors = [
            "//button[contains(text(), 'Filter')]",
            "//button[contains(text(), 'Saring')]",
            "//button[contains(@class, 'filter')]",
            "//button[contains(@id, 'filter')]"
        ]
        
        for selector in filter_selectors:
            try:
                elements = driver.find_elements(By.XPATH, selector)
                if elements:
                    filter_button = elements[0]
                    driver.execute_script("arguments[0].click();", filter_button)
                    time.sleep(2)
                    break
            except Exception as e:
                continue
        
        if not filter_button:
            print("[FILTER] Could not find filter button")
            return False
        
        # Select year
        year_input = None
        year_selectors = [
            f"//input[@value='{year}']",
            f"//option[@value='{year}']",
            f"//select//option[text()='{year}']"
        ]
        
        for selector in year_selectors:
            try:
                elements = driver.find_elements(By.XPATH, selector)
                if elements:
                    year_input = elements[0]
                    driver.execute_script("arguments[0].click();", year_input)
                    break
            except Exception as e:
                continue
          # Select period
        print(f"[FILTER] Selecting period: {period}")
        if period == "quarterly":
            # Pilih triwulan 1 (value="tw1", id="period0")
            period_selectors = [
                "//input[@value='tw1']",
                "//input[@id='period0']",
                "//input[@name='period'][@value='tw1']"
            ]
        else:
            # Pilih tahunan (value="audit", id="period3")
            period_selectors = [
                "//input[@value='audit']",
                "//input[@id='period3']",
                "//input[@name='period'][@value='audit']"
            ]
        
        for selector in period_selectors:
            try:
                elements = driver.find_elements(By.XPATH, selector)
                print(f"[FILTER] Period selector '{selector}': Found {len(elements)} elements")
                if elements:
                    driver.execute_script("arguments[0].click();", elements[0])
                    print(f"[FILTER] ✓ Successfully selected period: {period}")
                    break
            except Exception as e:
                print(f"[FILTER] Error with period selector '{selector}': {e}")
                continue
        
        # Select audit type (saham)
        audit_selectors = [
            "//input[@value='Saham']",
            "//input[contains(@id, 'saham')]"
        ]
        
        for selector in audit_selectors:
            try:
                elements = driver.find_elements(By.XPATH, selector)
                if elements:
                    driver.execute_script("arguments[0].click();", elements[0])
                    break
            except Exception as e:
                continue
        
        # Apply filter
        apply_selectors = [
            "//button[contains(text(), 'Terapkan')]",
            "//button[contains(text(), 'Apply')]"
        ]
        
        for selector in apply_selectors:
            try:
                elements = driver.find_elements(By.XPATH, selector)
                if elements:
                    driver.execute_script("arguments[0].click();", elements[0])
                    time.sleep(5)  # Wait for filter to be applied
                    break
            except Exception as e:
                continue
        
        print("[FILTER] Filters re-applied successfully")
        return True
        
    except Exception as e:
        print(f"[FILTER] Error re-applying filters: {e}")
        return False

def quick_check_xbrl_ticker(xbrl_path):
    """
    Quickly check what ticker is in an XBRL file without full processing
    
    Args:
        xbrl_path: Path to the XBRL file
        
    Returns:
        str: Ticker found in the file, or None if not found
    """
    try:
        with open(xbrl_path, 'r', encoding='utf-8') as xbrl_file:
            xbrl_content = xbrl_file.read()
            
        # Check if file is empty or too small
        if len(xbrl_content.strip()) < 100:
            return None
              # Quick check for ticker in identifier without full XML parsing
        # Look for <identifier scheme="http://www.idx.co.id">TICKER</identifier> pattern
        import re
        pattern = r'<identifier[^>]*scheme="http://www\.idx\.co\.id"[^>]*>([^<]+)</identifier>'
        match = re.search(pattern, xbrl_content)
        if match:
            found_identifier = match.group(1).strip()
            # Check if any of our target tickers is contained in this identifier
            target_tickers = ['AADI', 'AALI', 'ABBA', 'ABDA', 'ABMM']
            matched_ticker = is_valid_ticker(found_identifier, target_tickers)
            if matched_ticker:
                return matched_ticker
            return f"UNKNOWN({found_identifier})"  # Return with prefix if no match found
          # Fallback to full XML parsing if pattern not found
        try:
            xbrl_data = xmltodict.parse(xbrl_content)
            company_name = extract_company_name_from_xbrl(xbrl_data)
            if company_name:
                target_tickers = ['AADI', 'AALI', 'ABBA', 'ABDA', 'ABMM']
                matched_ticker = is_valid_ticker(company_name, target_tickers)
                return matched_ticker if matched_ticker else f"UNKNOWN({company_name})"
            return None
        except:
            return None
            
    except Exception as e:
        print(f"[QUICK_CHECK] Error checking XBRL ticker: {e}")
        return None

def is_valid_ticker(found_ticker, target_tickers):
    """
    Check if found ticker contains any of the target tickers
    
    Args:
        found_ticker: The ticker found in XBRL (e.g., "ABBA_APPROVER")
        target_tickers: List of target tickers (e.g., ['AADI', 'AALI', 'ABBA', 'ABDA', 'ABMM'])
    
    Returns:
        str: The matching target ticker if found, None otherwise
    """
    if not found_ticker:
        return None
    
    found_ticker = str(found_ticker).upper()
    
    for target in target_tickers:
        if target in found_ticker:
            print(f"[TICKER_MATCH] Found '{target}' in '{found_ticker}'")
            return target
    
    print(f"[TICKER_MATCH] No target ticker found in '{found_ticker}'")
    return None