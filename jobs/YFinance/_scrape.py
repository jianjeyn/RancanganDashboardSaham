import os
import time
import pymongo
import yfinance as yf
from dotenv import load_dotenv

load_dotenv('/opt/airflow/jobs/.env')

MONGODB_URI = os.getenv("MONGODB_URI")
MONGODB_DB = os.getenv("MONGODB_DB")

tickers = [
    'AADI.JK',
    'AALI.JK',
    'ABBA.JK',
    'ABDA.JK',
    'ABMM.JK',
]

try:
    client = pymongo.MongoClient(MONGODB_URI)
    db = client[MONGODB_DB]
    collection = db["yfinance"]

except Exception as e:
    print(f"[❌] Error connecting to MongoDB: {e}")
    exit(1)


for ticker in tickers:
    print(f"Collecting data for {ticker}...")

    try:
        saham = yf.Ticker(ticker)
        data = saham.history(period="1d")

        if data.empty:
            print(f"No data found for {ticker}. Skipping...")
            continue

        data.reset_index(inplace=True)
        json_saham = data.to_dict(orient="records")
        json_saham = [{"ticker": ticker, **record} for record in json_saham]

        collection.insert_many(json_saham)
        print(f"Data for {ticker} successfully inserted into MongoDB.")

    except Exception as e:
        print(f"[❌] Error collecting data for {ticker}: {e}")

    time.sleep(1)

print("Data collection completed.")
