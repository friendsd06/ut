from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.datasets import Dataset
from datetime import datetime, timedelta
import os
import time

# Import the Dataset
from datasets import file1_dataset

def ingest_data_source_1():
    """
    Ingest data from Source 1.
    """
    print("Ingesting data from Source 1...")
    time.sleep(70)
    # Replace with your actual ingestion logic
    os.makedirs("/opt/airflow/dataset-path", exist_ok=True)
    with open("/opt/airflow/dataset-path/file1.csv", "w") as f:
        f.write("id,name,value\n1,Alice,100\n2,Bob,200")

default_args = {
    'owner': 'your_name',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

with DAG(
    dag_id='ingestion_dag_1',
    default_args=default_args,
    description='Ingestion DAG for Data Source 1',
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=['ingestion'],
) as dag:

    ingest_task = PythonOperator(
        task_id='ingest_data_source_1',
        python_callable=ingest_data_source_1,
        outlets=[file1_dataset],  # Declare the Dataset produced by this task
    )
