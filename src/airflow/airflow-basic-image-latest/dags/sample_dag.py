from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
with DAG(
    'sample_dag',
    default_args=default_args,
    description='A simple sample DAG',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2024, 1, 1),
    catchup=False,
) as dag:

    # Task 1: Print Hello
    print_hello = BashOperator(
        task_id='print_hello',
        bash_command='echo "Hello, Airflow!"',
    )

    # Task 2: Sleep for 5 seconds
    sleep = BashOperator(
        task_id='sleep',
        bash_command='sleep 5',
    )

    # Task 3: Print Goodbye
    print_goodbye = BashOperator(
        task_id='print_goodbye',
        bash_command='echo "Goodbye, Airflow!"',
    )

    # Define Task Dependencies
    print_hello >> sleep >> print_goodbye