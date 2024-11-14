from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from datetime import datetime

# Define Python functions with meaningful print statements

def move_file_to_processing_directory():
    print("Moving file to Processing Directory...")

def check_trigger_most_automated():
    print("Checking if the trigger is Most Automated...")

def substitute():
    print("Substitute process initiated...")

def notify_event_hub():
    print("Notifying Event Hub/Levet about the current status...")

def check_block_unblock_status():
    print("Checking Block/Unblock status for substitution...")

def allow_substitution():
    print("Determining if substitution is allowed based on block status...")

def file_sanity_validation_checks():
    print("Performing file sanity validation checks...")

def run_transformation_derivation_validation():
    print("Running Transformation, Derivation & Validation on the data...")

def validate_breach_threshold():
    print("Checking if validation breach threshold is met...")

def persist_to_delta_table():
    print("Persisting data to Delta Table for further processing...")

def register_and_get_levid():
    print("Registering file and obtaining LevID...")

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}

# Define the DAG
with DAG(
        'data_pipeline_workflow',
        default_args=default_args,
        description='Data pipeline DAG based on provided diagram',
        schedule_interval=None,
        start_date=datetime(2023, 1, 1),
        catchup=False,
) as dag:

    # Define tasks
    start = DummyOperator(task_id='Start')

    sla_trigger_detected = DummyOperator(task_id='SLA_Trigger_detected')

    move_to_processing_directory_primary = PythonOperator(
        task_id='Move_the_file_to_Processing_Directory',
        python_callable=move_file_to_processing_directory
    )

    check_trigger_most_automated = PythonOperator(
        task_id='Is_the_trigger_Most_Automated',
        python_callable=check_trigger_most_automated
    )

    substitute_task = PythonOperator(
        task_id='Substitute',
        python_callable=substitute
    )

    notify_event_hub_levet = PythonOperator(
        task_id='Notify_Event_Hub_Levet',
        python_callable=notify_event_hub
    )

    check_block_unblock_status_task = PythonOperator(
        task_id='Check_Block_Unblock_status',
        python_callable=check_block_unblock_status
    )

    allow_substitution_task = PythonOperator(
        task_id='Allow_Substitution_or_not',
        python_callable=allow_substitution
    )

    file_sanity_validation_checks_task = PythonOperator(
        task_id='Did_the_file_sanity_validation_checks',
        python_callable=file_sanity_validation_checks
    )

    junction_node = DummyOperator(task_id='Junction_Node')

    register_get_levid = PythonOperator(
        task_id='Register_and_Get_LevID',
        python_callable=register_and_get_levid
    )

    allow_feed_processing = DummyOperator(task_id='Allow_Feed_Processing_or_not')

    run_transformation_derivation_validation_task = PythonOperator(
        task_id='Run_Transformation_Derivation_and_Validation',
        python_callable=run_transformation_derivation_validation
    )

    validation_breach_threshold_task = PythonOperator(
        task_id='Validation_breached_threshold',
        python_callable=validate_breach_threshold
    )

    persist_to_delta_table = PythonOperator(
        task_id='Persist_to_Delta_Table',
        python_callable=persist_to_delta_table
    )

    notify_event_hub_levet_final = PythonOperator(
        task_id='Notify_Event_Hub_Levet_Final',
        python_callable=notify_event_hub
    )

    # Define task dependencies to match the diagram
    start >> sla_trigger_detected >> move_to_processing_directory_primary
    sla_trigger_detected >> check_trigger_most_automated
    check_trigger_most_automated >> substitute_task >> notify_event_hub_levet
    substitute_task >> check_block_unblock_status_task >> allow_substitution_task
    allow_substitution_task >> run_transformation_derivation_validation_task >> validation_breach_threshold_task

    validation_breach_threshold_task >> persist_to_delta_table >> notify_event_hub_levet_final
    validation_breach_threshold_task >> allow_feed_processing
    validation_breach_threshold_task >> register_get_levid
    allow_feed_processing
