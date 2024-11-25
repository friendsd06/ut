from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.dummy import DummyOperator
from datetime import datetime, timedelta
import pandas as pd
import time
import logging
import hashlib

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
    'retries': 1
}

# Sample data
sample_data = pd.DataFrame({
    'product_id': ['P1', 'P2', 'P3'],
    'status': ['ACTIVE', 'INACTIVE', 'PENDING'],
    'region': ['NA', 'EU', 'APAC']
})

def validate_checksum(**context) -> str:
    """TSK1: Validate checksum"""
    logger.info("Starting checksum validation...")
    time.sleep(10)

    try:
        data_str = sample_data.to_string()
        calculated_checksum = hashlib.md5(data_str.encode()).hexdigest()
        expected_checksum = "sample_checksum"  # Replace with actual expected checksum

        if calculated_checksum == expected_checksum:
            logger.info("Checksum validation passed")
            return 'TSK3_lfv_generate'
        else:
            logger.error("Checksum validation failed")
            return 'handle_error'
    except Exception as e:
        logger.error(f"Error in checksum validation: {e}")
        return 'handle_error'

def handle_error(**context):
    """Handle validation errors"""
    logger.error("Processing error case")
    time.sleep(5)

def generate_lev_id(**context):
    """TSK3: Generate LEV IDs"""
    logger.info("Generating LEV IDs...")
    time.sleep(10)

    data = sample_data.copy()
    data['lev_id'] = 'LEV_' + data['product_id']

    context['task_instance'].xcom_push(key='processed_data', value=data.to_dict())
    logger.info("LEV IDs generated successfully")

def check_activation(**context) -> str:
    """TSK4: Check activation status"""
    logger.info("Checking activation status...")
    time.sleep(10)

    data = pd.DataFrame(context['task_instance'].xcom_pull(
        key='processed_data',
        task_ids='TSK3_lfv_generate'
    ))

    non_active = data[data['status'] != 'ACTIVE']
    if len(non_active) > 0:
        logger.info("Found non-active records")
        return 'TSK5_substitution_check'
    else:
        logger.info("All records active")
        return 'TSK6_dump_to_target'

def perform_substitution(**context):
    """TSK5: Perform substitutions"""
    logger.info("Performing substitutions...")
    time.sleep(10)

    data = pd.DataFrame(context['task_instance'].xcom_pull(
        key='processed_data',
        task_ids='TSK3_lfv_generate'
    ))

    substitutions = {
        'status': {
            'PENDING': 'IN_PROGRESS',
            'INACTIVE': 'DISABLED'
        },
        'region': {
            'NA': 'NORTH_AMERICA',
            'EU': 'EUROPE',
            'APAC': 'ASIA_PACIFIC'
        }
    }

    for column, mappings in substitutions.items():
        data[column] = data[column].replace(mappings)

    context['task_instance'].xcom_push(key='final_data', value=data.to_dict())
    logger.info("Substitutions completed")

def dump_data(**context):
    """TSK6: Dump data"""
    logger.info("Dumping data...")
    time.sleep(10)
    logger.info("Data dump completed")

def trigger_ingestion(**context):
    """TSK7: Trigger ingestion"""
    logger.info("Triggering ingestion...")
    time.sleep(10)
    logger.info("Ingestion triggered")

# Create DAG
with DAG(
        'simple_validation_pipeline',
        default_args=default_args,
        schedule_interval='@daily',
        catchup=False
) as dag:

    start = DummyOperator(task_id='start')

    checksum = BranchPythonOperator(
        task_id='TSK1_checksum',
        python_callable=validate_checksum,
        provide_context=True
    )

    handle_error = PythonOperator(
        task_id='handle_error',
        python_callable=handle_error,
        provide_context=True
    )

    generate_levs = PythonOperator(
        task_id='TSK3_lfv_generate',
        python_callable=generate_lev_id,
        provide_context=True
    )

    check_status = BranchPythonOperator(
        task_id='TSK4_activation_check',
        python_callable=check_activation,
        provide_context=True
    )

    perform_subs = PythonOperator(
        task_id='TSK5_substitution_check',
        python_callable=perform_substitution,
        provide_context=True
    )

    dump = PythonOperator(
        task_id='TSK6_dump_to_target',
        python_callable=dump_data,
        provide_context=True
    )

    trigger = PythonOperator(
        task_id='TSK7_trigger_ingestion',
        python_callable=trigger_ingestion,
        provide_context=True
    )

    end = DummyOperator(
        task_id='end',
        trigger_rule='none_failed'
    )

    # Define task dependencies
    start >> checksum >> [generate_levs, handle_error]
    generate_levs >> check_status >> [perform_subs, dump]
    perform_subs >> dump >> trigger >> end
    dump >> trigger >> end
    handle_error >> end