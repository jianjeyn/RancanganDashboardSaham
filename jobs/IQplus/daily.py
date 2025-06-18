import os
import subprocess
from dotenv import load_dotenv

# Load environment variables
load_dotenv('/opt/airflow/jobs/.env')

MONGODB_URI = os.getenv("MONGODB_URI")
MONGODB_DB = os.getenv("MONGODB_DB")

# Jalankan _scrap.py untuk scraping dan simpan ke MongoDB
subprocess.run(["python", "jobs/IQplus/_scrap.py"], check=True)
