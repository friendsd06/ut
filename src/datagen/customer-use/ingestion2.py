from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def ingest_customer_data(**context):
    """
    Function to ingest customer data.
    """
    print("Starting customer data ingestion...")
    # Add your customer data ingestion logic here
    print("Customer data ingestion completed successfully.")

dag = DAG(
    'ingestion_customer_data_dag',
    default_args=default_args,
    description='DAG to ingest customer data',
    schedule_interval='@daily',
    catchup=False,
)

ingest_customer_data_task = PythonOperator(
    task_id='ingest_customer_data',
    python_callable=ingest_customer_data,
    dag=dag,
)

trigger_orchestrator_task = TriggerDagRunOperator(
    task_id='trigger_orchestrator',
    trigger_dag_id='orchestrator_dag',
    conf={'dataset': 'customer_data'},
    dag=dag,
)

ingest_customer_data_task >> trigger_orchestrator_task
