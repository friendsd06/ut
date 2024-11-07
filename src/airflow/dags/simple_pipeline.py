from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
import time

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email': ['your@email.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

def print_hello():
    while True:
        print('Hello from first task!')
        time.sleep(2)  # Sleep for 2 seconds
    return 'Hello from first task!'

def run_date_command():
    while True:
        from subprocess import check_output
        print(check_output(['date']).decode())
        time.sleep(2)

def print_end():
    while True:
        print("Pipeline finished!")
        time.sleep(2)

with DAG(
        'simple_pipeline_continuous',
        default_args=default_args,
        description='A simple Airflow pipeline running every 2 seconds',
        schedule_interval=timedelta(minutes=1),  # This will start the continuous process every minute if it fails
        catchup=False
) as dag:

    t1 = PythonOperator(
        task_id='continuous_hello',
        python_callable=print_hello,
    )

    t2 = PythonOperator(
        task_id='continuous_date',
        python_callable=run_date_command,
    )

    t3 = PythonOperator(
        task_id='continuous_end',
        python_callable=print_end,
    )

    t1 >> t2 >> t3