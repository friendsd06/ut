from airflow.decorators import dag, task
from airflow.operators.dummy import DummyOperator
from airflow.operators.email import EmailOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.sensors.filesystem import FileSensor
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.utils.task_group import TaskGroup
from airflow.utils.trigger_rule import TriggerRule
from airflow.exceptions import AirflowSkipException
from datetime import datetime, timedelta
import random
import logging

# Set up logging
logger = logging.getLogger("airflow.task")

# Default arguments for the DAG with retry logic and callbacks
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': True,
    'email': ['alert@example.com'],
    'retry_delay': timedelta(minutes=5),
    'retries': 3,
}

# Define a failure callback function
def task_failure_alert(context):
    logger.error(f"Task {context['task_instance'].task_id} failed.")
    # You can add code here to send alerts (email, Slack, etc.)

# Define the DAG using the TaskFlow API
@dag(
    default_args=default_args,
    schedule_interval=None,
    start_date=datetime(2023, 1, 1),
    catchup=False,
    description='Advanced Data Pipeline DAG with real-time elements and additional features',
)
def advanced_data_pipeline_workflow():
    # Start task
    @task
    def start():
        logger.info("Start of the DAG")

    # External dependency check using ExternalTaskSensor
    external_dependency = ExternalTaskSensor(
        task_id='external_dependency_check',
        external_dag_id='dependency_dag',
        external_task_id='dependency_task',
        timeout=600,
        poke_interval=60,
        mode='poke',
        on_failure_callback=task_failure_alert,
    )

    # Wait for files to appear using FileSensor
    wait_for_files = FileSensor(
        task_id='wait_for_files',
        fs_conn_id='fs_default',
        filepath='/path/to/processing_directory',  # Replace with actual directory
        poke_interval=10,
        timeout=600,
        on_failure_callback=task_failure_alert,
    )

    # Get a dynamic list of files to process
    @task
    def get_file_list():
        # Simulate getting a list of files
        file_list = [f"file_{i}.csv" for i in range(1, 6)]
        logger.info(f"Files to process: {file_list}")
        return file_list

    # Move files to processing directory using dynamic task mapping
    @task
    def move_file_to_processing_directory(file_name: str):
        logger.info(f"Moving {file_name} to Processing Directory...")
        # Simulate file movement with a chance of failure
        if random.choice([True, False]):
            raise ValueError(f"File {file_name} not found in directory")
        logger.info(f"File {file_name} moved successfully")
        return file_name

    # File sanity validation checks with retries and failure callback
    @task(retries=2, on_failure_callback=task_failure_alert)
    def file_sanity_validation_checks():
        logger.info("Performing file sanity validation checks...")
        if random.choice([True, False]):
            raise ValueError("File sanity check failed")

    # Check if the trigger is Most Automated
    @task.branch
    def check_trigger_most_automated():
        decision = random.choice(["substitute_task", "continue_processing"])
        logger.info(f"Decision: {decision}")
        return decision

    # Substitute task
    @task
    def substitute_task():
        logger.info("Substitute process initiated...")

    # Notify Event Hub/Levet with SLA and failure callback
    @task(sla=timedelta(minutes=10), on_failure_callback=task_failure_alert)
    def notify_event_hub():
        logger.info("Notifying Event Hub/Levet about the current status...")

    # Email notification using EmailOperator
    email_notification = EmailOperator(
        task_id='email_notification',
        to='example@example.com',
        subject='Substitute Task Completed',
        html_content="The Substitute task has been successfully completed and notified.",
    )

    # Check Block/Unblock status
    @task
    def check_block_unblock_status():
        status = random.choice(["Allow", "Block"])
        logger.info(f"Block/Unblock Status: {status}")
        return status

    # Allow substitution or not based on status
    @task.branch
    def allow_substitution_task(status: str):
        if status == "Allow":
            logger.info("Substitution allowed based on block status.")
            # Return the full task ID including the TaskGroup
            return 'transformation_and_validation_group.run_transformation_derivation_validation'
        else:
            logger.info("Substitution not allowed.")
            return 'end_substitution'

    # Dummy task to end substitution path without creating a cycle
    end_substitution = DummyOperator(task_id='end_substitution')

    # Transformation and Validation Task Group
    with TaskGroup("transformation_and_validation_group") as transformation_group:
        @task
        def run_transformation_derivation_validation():
            logger.info("Running Transformation, Derivation & Validation on the data...")

        @task.branch
        def validate_breach_threshold():
            result = random.choice(["persist_to_delta_table", "notify_event_hub_levet_final"])
            logger.info(f"Validation Result: {result}")
            return result

        # Instantiate tasks within the TaskGroup
        run_transformation = run_transformation_derivation_validation()
        validation_decision = validate_breach_threshold()

    # Persist to Delta Table
    @task
    def persist_to_delta_table():
        logger.info("Persisting data to Delta Table for further processing...")

    # Final notification
    @task
    def notify_event_hub_levet_final():
        logger.info("Notifying Event Hub/Levet about final status...")

    # Final email notification
    final_email_notification = EmailOperator(
        task_id='final_email_notification',
        to='example@example.com',
        subject='Data Pipeline Completed',
        html_content="The data pipeline has completed successfully.",
    )

    # Trigger downstream workflow
    trigger_downstream = TriggerDagRunOperator(
        task_id='trigger_downstream_workflow',
        trigger_dag_id='downstream_data_pipeline',
        wait_for_completion=False,
    )

    # Define the DAG flow
    # Initial tasks
    start_task = start()
    start_task >> external_dependency >> wait_for_files
    file_list = get_file_list()
    wait_for_files >> file_list

    # Dynamic task mapping for moving files
    move_files_to_processing = move_file_to_processing_directory.expand(file_name=file_list)
    file_list >> move_files_to_processing

    # File sanity validation
    file_sanity_checks = file_sanity_validation_checks()
    move_files_to_processing >> file_sanity_checks

    # Check trigger and branch
    decision = check_trigger_most_automated()
    file_sanity_checks >> decision

    # Define possible paths after the decision
    substitute = substitute_task()
    continue_processing = DummyOperator(task_id='continue_processing')
    decision >> substitute
    decision >> continue_processing

    # Substitute path
    notify_event = notify_event_hub()
    substitute >> notify_event
    notify_event >> email_notification

    check_block_status = check_block_unblock_status()
    substitute >> check_block_status

    allow_substitution = allow_substitution_task(check_block_status)
    check_block_status >> allow_substitution

    # Branching after allow_substitution
    allow_substitution >> [
        transformation_group,
        end_substitution
    ]

    # Transformation and Validation Group dependencies
    run_transformation >> validation_decision

    # Branching after validation decision
    validation_decision >> {
        'persist_to_delta_table': persist_to_delta_table(),
        'notify_event_hub_levet_final': notify_event_hub_levet_final(),
    }

    # Final steps
    persist_to_delta_table() >> final_email_notification >> trigger_downstream
    notify_event_hub_levet_final() >> final_email_notification >> trigger_downstream
    end_substitution >> final_email_notification >> trigger_downstream
    continue_processing >> final_email_notification >> trigger_downstream

# Instantiate the DAG
advanced_data_pipeline_workflow_dag = advanced_data_pipeline_workflow()
