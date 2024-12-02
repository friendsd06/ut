from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.utils.dates import days_ago

default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
}

def generate_odd_event(**kwargs):
    # Fixed odd event
    event = 3
    # Push the event to XCom
    kwargs['ti'].xcom_push(key='event', value=event)
    return event

dag = DAG(
    'generate_odd_task_dag',
    default_args=default_args,
    description='Triggers even_odd_dag with an odd event',
    schedule_interval=None,
    catchup=False,
)

generate_odd_event_task = PythonOperator(
    task_id='generate_odd_event',
    python_callable=generate_odd_event,
    dag=dag,
)

trigger_odd_task = TriggerDagRunOperator(
    task_id='trigger_odd_odd_dag',
    trigger_dag_id='even_odd_dag',
    conf={
        'event': '{{ ti.xcom_pull(task_ids="generate_odd_event") }}'
    },
    dag=dag,
)

generate_odd_event_task >> trigger_odd_task