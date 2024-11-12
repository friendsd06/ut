# dags/ingestion_dag_3.py

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.datasets import Dataset
from datetime import datetime, timedelta
import os

# Import the Dataset
from datasets import file3_dataset

def ingest_data_source_3():
    """
    Ingest data from Source 3.
    """
    print("Ingesting data from Source 3...")
    # Replace the following with your actual ingestion logic
    os.makedirs("/opt/airflow/dataset-path", exist_ok=True)
    with open("/opt/airflow/dataset-path/file3.csv", "w") as f:
        f.write("id,product,quantity\n1,Widget,50\n2,Gadget,75")

default_args = {
    'owner': 'your_name',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

with DAG(
    dag_id='ingestion_dag_3',
    default_args=default_args,
    description='Ingestion DAG for Data Source 3',
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=['ingestion'],
) as dag:

    ingest_task = PythonOperator(
        task_id='ingest_data_source_3',
        python_callable=ingest_data_source_3,
        outlets=[file3_dataset],  # Declare the Dataset produced by this task
    )
