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

stocks = [
    'AALI.JK',
    'ABBA.JK',
    'ABDA.JK',
    'ABMM.JK',
    'AADI.JK',
]

try:
    client = pymongo.MongoClient(MONGODB_URI)
    db = client[MONGODB_DB]
    collection = db["iqplus"]

except Exception as e:
    print(f"[❌] Error connecting to MongoDB: {e}")
    exit(1)


def init_driver():
    options = webdriver.ChromeOptions()
    options.add_argument("--headless")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_argument("--disable-gpu")
    options.add_argument(f"--user-data-dir={tempfile.mkdtemp()}")
    return webdriver.Chrome(options=options)


def summarize(teks):
    try:
        word_count = len(teks.split())
        max_len = min(150, word_count // 2 + 20)
        min_len = min(80, word_count // 4)
        summary = summarizer(teks, max_length=max_len, min_length=min_len, do_sample=False)
        return summary[0]['summary_text']

    except Exception as e:
        print(f"[❌] Error during summarization: {e}")
        return teks


def get_all_news(driver, stock):
    try:
        WebDriverWait(driver, 10).until(EC.presence_of_all_elements_located(By.XPATH, "//ul[@class='news']/li"))
        news_date = driver.find_elements(By.XPATH, "//ul[@class='news']/li/b")
        news_link = driver.find_elements(By.XPATH, "//ul[@class='news']/li/a")
        print(f"[ℹ️] Found {len(news_link)} articles for {stock}.")

        links = []
        for i, link in enumerate(news_link):
            if link.text.startswith(f"{stock.split('.')[0]}: "):
                try:
                    links.append({
                        "title": link.text.strip(),
                        "URL": link.get_attribute("href"),
                        "date": news_date[i].text.strip() if i < len(news_date) else "undefined"
                    })

                except Exception as e:
                    print(f"[❌] Error capturing link: {e}")

        print(f"[ℹ️] Total links captured: {len(links)}")
        return links

    except Exception as e:
        print(f"[❌] Error fetching articles for {stock}: {e}")
        return []


def collect_data():
    driver = init_driver()

    url = f"http://www.iqplus.info"
    print(f"[📄] Membuka halaman: {url}")

    try:
        driver.get(url)
        time.sleep(2)

        search_form = driver.find_element(By.XPATH, "//form[@class='fsearch']/input[@name='search']")
        for stock in stocks:
            search_form.clear()
            search_form.send_keys(stock)
            search_form.submit()
            time.sleep(2)

            news_links = get_all_news(driver, stock)
            if not news_links:
                print(f"[ℹ️] No news found for {stock}.")
                continue

            print(f"[ℹ️] Total news found for {stock}: {len(news_links)}")

        for link in news_links:
            try:
                if collection.find_one({"url": link["URL"]}):
                    print(f"[⏩] Skip, already exists: {link['title']}")
                    continue

                driver.get(link["URL"])
                WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.XPATH, "//div[@id='zoomthis']")))
                content = driver.find_element(By.XPATH, "//div[@id='zoomthis']").text.strip()
                link["content"] = content
                link["summary"] = summarize(content)
                collection.insert_one(link)
                print(f"[✅] Saved: {link['title']}")

            except Exception as e:
                print(f"[❌] Error processing article '{link['title']}': {e}")

            time.sleep(2)

    except Exception as e:
        print(f"[❌] Gagal membuka halaman {url}: {e}")

    driver.quit()
    print("✅ Semua berita selesai diproses!")


if __name__ == "__main__":
    collect_data()
