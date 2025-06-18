import os
import time
import pymongo
import tempfile
from dotenv import load_dotenv
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from transformers import pipeline

# Load environment variables
load_dotenv('/opt/airflow/jobs/.env')

MONGODB_URI = os.getenv("MONGODB_URI")
MONGODB_DB = os.getenv("MONGODB_DB")

# Summarizer dari Hugging Face Transformers
summarizer = pipeline("summarization", model="facebook/bart-large-cnn")


def ringkas_berita(teks):
    try:
        word_count = len(teks.split())
        max_len = min(150, word_count // 2 + 20)
        min_len = min(80, word_count // 4)
        summary = summarizer(teks, max_length=max_len, min_length=min_len, do_sample=False)
        return summary[0]['summary_text']
    except Exception as e:
        print(f"[❌] Gagal meringkas berita: {e}")
        return teks


def init_driver():
    options = webdriver.ChromeOptions()
    options.add_argument("--headless")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_argument("--disable-gpu")
    options.add_argument(f"--user-data-dir={tempfile.mkdtemp()}")
    return webdriver.Chrome(options=options)


def get_all_articles(driver):
    try:
        WebDriverWait(driver, 10).until(EC.presence_of_all_elements_located(
            (By.XPATH, "//div[@class='box']//ul[@class='news']/li/a")))
        berita_elements = driver.find_elements(By.XPATH, "//div[@class='box']//ul[@class='news']/li/a")
        tanggal_elements = driver.find_elements(By.XPATH, "//div[@class='box']//ul[@class='news']/li/b")

        berita_links = []
        for i, el in enumerate(berita_elements):
            try:
                berita_links.append({
                    "judul": el.text.strip(),
                    "url": el.get_attribute("href"),
                    "tanggal": tanggal_elements[i].text.strip() if i < len(tanggal_elements) else "Tidak Diketahui"
                })
            except Exception as e:
                print(f"[❌] Gagal parsing berita ke-{i}: {e}")
        return berita_links
    except Exception as e:
        print(f"[❌] Gagal mengambil daftar berita: {e}")
        return []


def scrape_iqplus(max_page=50, max_duration=3600):
    try:
        client = pymongo.MongoClient(MONGODB_URI)
        db = client[MONGODB_DB]
        berita_collection = db["BERITA_IQPLUS"]
    except Exception as e:
        print(f"[❌] Gagal konek ke MongoDB: {e}")
        return

    driver = init_driver()
    start_time = time.time()

    for page in range(1, max_page + 1):
        if time.time() - start_time > max_duration:
            print("⏰ Waktu maksimum tercapai. Menghentikan scraping.")
            break

        url = f"http://www.iqplus.info/news/stock_news/go-to-page,{page}.html"
        print(f"[📄] Membuka halaman: {url}")
        try:
            driver.get(url)
            time.sleep(2)
            berita_list = get_all_articles(driver)
        except Exception as e:
            print(f"[❌] Gagal membuka halaman {page}: {e}")
            continue

        print(f"[ℹ️] Ditemukan {len(berita_list)} berita di halaman {page}.")

        for berita in berita_list:
            if time.time() - start_time > max_duration:
                break

            try:
                # Skip jika berita sudah ada (berdasarkan URL)
                if berita_collection.find_one({"url": berita["url"]}):
                    print(f"[⏩] Lewati, sudah ada: {berita['judul']}")
                    continue

                driver.get(berita["url"])
                WebDriverWait(driver, 10).until(
                    EC.presence_of_element_located((By.XPATH, "//div[@id='zoomthis']"))
                )
                isi = driver.find_element(By.XPATH, "//div[@id='zoomthis']").text.strip()
                berita["isi"] = isi
                berita["ringkasan"] = ringkas_berita(isi)
                berita_collection.insert_one(berita)
                print(f"[✅] Disimpan: {berita['judul']}")
            except Exception as e:
                print(f"[❌] Gagal memproses berita '{berita['judul']}': {e}")
            time.sleep(2)

    driver.quit()
    print("✅ Semua berita selesai diproses!")


if __name__ == "__main__":
    scrape_iqplus()
