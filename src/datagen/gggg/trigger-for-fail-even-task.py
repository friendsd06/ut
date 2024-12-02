from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.utils.dates import days_ago

default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
}

dag = DAG(
    'fail_even_task_dag',
    default_args=default_args,
    description='Triggers even_odd_dag with an even event and fail_even=true',
    schedule_interval=None,
    catchup=False,
)

trigger_fail_even_task = TriggerDagRunOperator(
    task_id='trigger_fail_even_odd_dag',
    trigger_dag_id='even_odd_dag',
    conf={"event": 2, "fail_even": "false"},
    dag=dag,
)
