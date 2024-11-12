# dags/ingestion_dag_2.py

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.datasets import Dataset
from datetime import datetime, timedelta
import os

# Import the Dataset
from datasets import file2_dataset

def ingest_data_source_2():
    """
    Ingest data from Source 2.
    """
    print("Ingesting data from Source 2...")
    # Replace the following with your actual ingestion logic
    os.makedirs("/opt/airflow/dataset-path", exist_ok=True)
    with open("/opt/airflow/dataset-path/file2.csv", "w") as f:
        f.write("id,category,amount\n1,Electronics,500\n2,Furniture,800")

default_args = {
    'owner': 'your_name',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=3),
}

with DAG(
    dag_id='ingestion_dag_2',
    default_args=default_args,
    description='Ingestion DAG for Data Source 2',
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=['ingestion'],
) as dag:

    ingest_task = PythonOperator(
        task_id='ingest_data_source_2',
        python_callable=ingest_data_source_2,
        outlets=[file2_dataset],  # Declare the Dataset produced by this task
    )
