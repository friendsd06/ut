from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

# Define default arguments
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 11, 7),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
with DAG(
        dag_id='test_dag',
        default_args=default_args,
        schedule_interval='@daily',
        catchup=False,
) as dag:

    # Define the task
    t1 = BashOperator(
        task_id='print_date',
        bash_command='date',
    )

    # You can define more tasks here

    # Set task dependencies (if you have more than one task)
    t1

