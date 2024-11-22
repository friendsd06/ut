from airflow import DAG
from airflow.operators.python import BranchPythonOperator, PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.models import Variable
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'start_date': datetime(2024, 1, 1),
    'retries': 0,  # No retries for orchestrator tasks
}

def check_ingestion_status(**context):
    """
    Checks if all ingestion datasets have been ingested.
    """
    dataset = context['dag_run'].conf.get('dataset')
    ingestion_status = Variable.get('ingestion_status', deserialize_json=True, default_var={})

    # Mark the current dataset as ingested
    ingestion_status[dataset] = True
    Variable.set('ingestion_status', ingestion_status, serialize_json=True)

    # Check if all datasets are ingested
    required_datasets = {'loan_data', 'customer_data', 'transactions_data'}
    if required_datasets.issubset(ingestion_status.keys()) and all(ingestion_status.values()):
        return 'trigger_enrichment_task'
    else:
        return 'skip_enrichment_task'

def reset_ingestion_status(**context):
    """
    Resets the ingestion status Variable.
    """
    Variable.set('ingestion_status', {}, serialize_json=True)
    print("Ingestion status has been reset.")

def skip_enrichment(**context):
    """
    Logs that enrichment is being skipped.
    """
    print("Not all datasets are ingested yet. Skipping enrichment.")

dag = DAG(
    'orchestrator_dag',
    default_args=default_args,
    description='Orchestrator DAG that triggers enrichment after all datasets are ingested',
    schedule_interval=None,  # Only triggered by other DAGs
    catchup=False,
)

check_ingestion_status_task = BranchPythonOperator(
    task_id='check_ingestion_status',
    python_callable=check_ingestion_status,
    provide_context=True,
    dag=dag,
)

trigger_enrichment_task = TriggerDagRunOperator(
    task_id='trigger_enrichment_task',
    trigger_dag_id='loan_enrichment_dag',
    reset_dag_run=True,
    wait_for_completion=False,
    dag=dag,
)

skip_enrichment_task = PythonOperator(
    task_id='skip_enrichment_task',
    python_callable=skip_enrichment,
    dag=dag,
)

reset_ingestion_status_task = PythonOperator(
    task_id='reset_ingestion_status',
    python_callable=reset_ingestion_status,
    trigger_rule='all_done',
    dag=dag,
)

check_ingestion_status_task >> [trigger_enrichment_task, skip_enrichment_task]
trigger_enrichment_task >> reset_ingestion_status_task
skip_enrichment_task >> reset_ingestion_status_task