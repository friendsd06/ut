from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import psycopg2
import json

def record_business_context(**context):
    """Record business context for the DAG run"""
    dag_id = context['dag'].dag_id
    execution_date = context['execution_date']

    business_context = {
        'dag_id': dag_id,
        'execution_date': execution_date,
        'business_unit': 'Sales',
        'data_owner': 'John Doe',
        'cost_center': 'CC001',
        'priority': 'HIGH',
        'impact_level': 'P1',
        'process_name': 'Daily Sales Processing',
        'upstream_systems': ['Salesforce', 'SAP'],
        'downstream_systems': ['DataWarehouse', 'Tableau']
    }

    # Insert into business_context table
    insert_metadata('business_context', business_context)

def track_data_lineage(**context):
    """Track data lineage for the task"""
    task_instance = context['task_instance']
    dag_id = context['dag'].dag_id
    task_id = context['task'].task_id
    execution_date = context['execution_date']

    lineage_data = {
        'dag_id': dag_id,
        'task_id': task_id,
        'execution_date': execution_date,
        'source_system': 'Salesforce',
        'source_table': 'Opportunities',
        'target_system': 'DataWarehouse',
        'target_table': 'dim_sales',
        'records_processed': 15000,
        'transformation_type': 'FULL_LOAD',
        'data_quality_score': 98.5
    }

    # Insert into data_lineage table
    insert_metadata('data_lineage', lineage_data)

def monitor_sla(**context):
    """Monitor SLA for the DAG run"""
    dag_id = context['dag'].dag_id
    execution_date = context['execution_date']

    sla_data = {
        'dag_id': dag_id,
        'execution_date': execution_date,
        'sla_type': 'DAILY_SALES_PROCESSING',
        'expected_start_time': context['execution_date'],
        'actual_start_time': datetime.now(),
        'expected_end_time': context['execution_date'] + timedelta(hours=1),
        'actual_end_time': None,
        'sla_status': 'IN_PROGRESS'
    }

    # Insert into sla_monitoring table
    insert_metadata('sla_monitoring', sla_data)

# Example DAG using custom metadata
with DAG(
        'sales_processing_with_metadata',
        start_date=datetime(2024, 1, 1),
        schedule_interval='@daily'
) as dag:

    record_context = PythonOperator(
        task_id='record_business_context',
        python_callable=record_business_context,
        provide_context=True
    )

    track_lineage = PythonOperator(
        task_id='track_data_lineage',
        python_callable=track_data_lineage,
        provide_context=True
    )

    monitor_sla_task = PythonOperator(
        task_id='monitor_sla',
        python_callable=monitor_sla,
        provide_context=True
    )

    record_context >> track_lineage >> monitor_sla_task