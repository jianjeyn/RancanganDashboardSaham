import subprocess
from airflow import DAG
from datetime import datetime
from airflow.operators.python import PythonOperator

def collect_data():
    subprocess.run(["python", "jobs/YFinance/monthly.py"], check=True)

with DAG(
    dag_id="yfinance_monthly",
    start_date=datetime(2024, 1, 1),
    schedule="0 20 31 * *",
    catchup=False
) as dag:
    scrap = PythonOperator(
        task_id="collect_data",
        python_callable=collect_data,
    )
