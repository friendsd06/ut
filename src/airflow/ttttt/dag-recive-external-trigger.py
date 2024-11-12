# File: dags/example_external_trigger_dag.py
# This is the DAG that will be triggered externally

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.email import EmailOperator
from datetime import datetime, timedelta
import json
import logging

# Set up logger
logger = logging.getLogger(__name__)

def validate_conf(**context):
    """Validate the configuration passed to the DAG"""
    conf = context['dag_run'].conf
    logger.info(f"Received configuration: {conf}")

    required_keys = ['data_date', 'parameters']

    if not conf:
        raise ValueError("No configuration provided")

    for key in required_keys:
        if key not in conf:
            raise ValueError(f"Missing required key: {key}")

    # Validate parameters
    params = conf['parameters']
    if 'source' not in params:
        raise ValueError("Source must be specified in parameters")

    return conf

def process_data(**context):
    """Process the data based on configuration"""
    conf = context['dag_run'].conf
    params = conf['parameters']

    logger.info(f"Processing data for date: {conf['data_date']}")
    logger.info(f"Source system: {params['source']}")
    logger.info(f"Process type: {params.get('process_type', 'incremental')}")

    # Add your processing logic here
    result = {
        "processed_date": conf['data_date'],
        "source": params['source'],
        "status": "success",
        "records_processed": 1000  # Example metric
    }

    # Push result to XCom for downstream tasks
    context['task_instance'].xcom_push(key='processing_result', value=result)
    return result

def send_notification(**context):
    """Prepare notification based on processing results"""
    conf = context['dag_run'].conf
    result = context['task_instance'].xcom_pull(task_ids='process_data', key='processing_result')

    notification_email = conf['parameters'].get('email_notification')
    if notification_email:
        message = f"""
        Data Processing Complete

        Date: {result['processed_date']}
        Source: {result['source']}
        Status: {result['status']}
        Records Processed: {result['records_processed']}
        """
        return message
    return None

# Define default arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Create DAG
with DAG(
        'example_external_trigger_dag',
        default_args=default_args,
        description='Example DAG for external triggering',
        schedule_interval=None,  # Only triggered externally
        start_date=datetime(2024, 1, 1),
        catchup=False,
        tags=['external', 'example'],
) as dag:

    # Task 1: Validate Configuration
    validate_task = PythonOperator(
        task_id='validate_configuration',
        python_callable=validate_conf,
        provide_context=True,
    )

    # Task 2: Process Data
    process_task = PythonOperator(
        task_id='process_data',
        python_callable=process_data,
        provide_context=True,
    )

    # Task 3: Prepare Notification
    notification_task = PythonOperator(
        task_id='prepare_notification',
        python_callable=send_notification,
        provide_context=True,
    )

    # Task 4: Send Email Notification
    email_task = EmailOperator(
        task_id='send_email_notification',
        to="{{ dag_run.conf['parameters']['email_notification'] }}",
        subject='Data Processing Complete - {{ ds }}',
        html_content="{{ task_instance.xcom_pull(task_ids='prepare_notification') }}",
    )

    # Set task dependencies
    validate_task >> process_task >> notification_task >> email_task