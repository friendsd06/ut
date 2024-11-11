from airflow.operators.python_operator import PythonOperator
from datetime import timedelta


process_data = PythonOperator(
    task_id="process_data",
    python_callable=process_data_fn,
    retries=3,
    retry_delay=timedelta(seconds=300),
    pool="data_pool",
    priority_weight=2,
    doc_md="""Process daily data""",
)
