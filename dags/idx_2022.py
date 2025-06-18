import subprocess
from airflow import DAG
from datetime import datetime
from airflow.operators.python import PythonOperator

def collect_data():
    subprocess.run(["python", "jobs/idx/idx_2022.py"], check=True)

with DAG(
    dag_id="idx_2022",
    start_date=datetime(2022, 1, 1),
    schedule="0 21 15 1 *",  # Run annually on January 15th at 21:00 (after annual reports are published)
    catchup=False
) as dag:
    scrap = PythonOperator(
        task_id="collect_data",
        python_callable=collect_data,
    )
