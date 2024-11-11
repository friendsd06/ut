from airflow import DAG
from datetime import datetime, timedelta

from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

def print_date(msg):
    """Example Python callable"""
    from datetime import datetime
    print(f"{msg}: {datetime.now()}")


with DAG(
    dag_id='example_dag',
    description='Example DAG with Python and Bash tasks',
    schedule_interval='@daily',
    start_date=datetime.fromisoformat('2024-01-01T00:00:00'),
    tags=['example', 'demo'],
    **{'catchup': False, 'max_active_runs': 1}
) as dag:
    hello_bash = BashOperator(
        task_id='hello_bash',
        **{'bash_command': "echo 'Hello World'"}
    )
    python_task = PythonOperator(
        task_id='python_task',
        **{'python_callable': 'print_date', 'op_kwargs': {'msg': 'Current date is'}}
    )

    hello_bash >> python_task
