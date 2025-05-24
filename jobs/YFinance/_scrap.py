import os
import time
import pymongo
import yfinance as yf
from dotenv import load_dotenv

load_dotenv('/opt/airflow/jobs/.env')

MONGODB_URI = os.getenv("MONGODB_URI")
MONGODB_DB = os.getenv("MONGODB_DB")

client = pymongo.MongoClient(MONGODB_URI)
db = client[MONGODB_DB]
collection = db["yfinance"]

tickers = [
    'AADI.JK',
    'AALI.JK',
    'ABBA.JK',
    'ABDA.JK',
    'ABMM.JK',
]

for ticker in tickers:
    print(f"Mengambil data saham {ticker} dari yfinance...")

    try:
        saham = yf.Ticker(ticker)
        data = saham.history(period="1d")

        if data.empty:
            print(f"Data kosong untuk {ticker}")
            continue

        data.reset_index(inplace=True)
        json_saham = data.to_dict(orient="records")
        json_saham = [{"ticker": ticker, **record} for record in json_saham]

        collection.insert_many(json_saham)
        print(f"Data saham {ticker} berhasil disimpan ke MongoDB!")

    except Exception as e:
        print(f"ERROR: Gagal mengambil data {ticker}: {e}")

    time.sleep(1)

print("Semua data saham selesai diproses!")
