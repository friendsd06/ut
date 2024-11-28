from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators import DataTransferOperator,FileCountSensor
from airflow.hooks import MySQLToPostgresHook
from airflow.operators.python_operator import PythonOperator


dag = DAG('plugins_dag', schedule_interval=timedelta(1), start_date=datetime(2020, 1, 25), catchup=False)

def trigger_hook():
    MySQLToPostgresHook().copy_table('mysql_conn', 'postgres_conn')
    print("done")

t1 = PythonOperator(
    task_id = 'mysql_to_postgres',
    python_callable = trigger_hook,
    dag = dag
)
