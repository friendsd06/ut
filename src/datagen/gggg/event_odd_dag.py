from airflow import DAG
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.utils.dates import days_ago
from airflow.models import DagRun
from airflow.utils.db import create_session
from airflow.utils.state import State

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': True,  # Prevents tasks from running if the previous instance of the same task failed
    'start_date': days_ago(1),
    'retries': 0,
}

# Define the DAG
dag = DAG(
    'even_odd_dag',
    default_args=default_args,
    description='A DAG that executes tasks based on even or odd event parameter and fails tasks based on external events',
    schedule_interval=None,  # Triggers only via external event or manual intervention
    catchup=False,
)

def check_event(**kwargs):
    ti = kwargs['ti']
    logger = ti.log
    dag = kwargs['dag']
    dag_run = kwargs['dag_run']
    conf = dag_run.conf or {}
    logger.info(f"Received configuration in check_event: {conf}, type: {type(conf)}")

    # Get the previous DAG run
    with create_session() as session:
        previous_dag_run = session.query(DagRun).filter(
            DagRun.dag_id == dag.dag_id,
            DagRun.execution_date < dag_run.execution_date
        ).order_by(DagRun.execution_date.desc()).first()

    if previous_dag_run:
        previous_state = previous_dag_run.get_state()
        logger.info(f"Previous DAG run state: {previous_state}")
        if previous_state != State.SUCCESS:
            logger.error("Previous DAG run did not succeed. Aborting current run.")
            raise Exception("Previous DAG run did not succeed. Aborting current run.")
    else:
        logger.info("No previous DAG run found. Proceeding with current run.")

    # Existing code for event checking
    event = conf.get('event')
    if event is None:
        logger.error("No 'event' parameter provided.")
        raise ValueError("No 'event' parameter provided.")
    try:
        event = int(event)
    except ValueError:
        logger.error("Invalid 'event' parameter. Must be an integer.")
        raise ValueError("Invalid 'event' parameter. Must be an integer.")

    if event % 2 == 0:
        logger.info("Event is even. Routing to even_task.")
        return 'even_task'
    else:
        logger.info("Event is odd. Routing to odd_task.")
        return 'odd_task'

# Task to check if 'event' is even or odd
check_event_task = BranchPythonOperator(
    task_id='check_event',
    python_callable=check_event,
    dag=dag,
)

# The rest of your tasks remain the same
# Task to execute if 'event' is even
def even_task(**kwargs):
    # ... existing code ...
    pass

even_task_operator = PythonOperator(
    task_id='even_task',
    python_callable=even_task,
    dag=dag,
)

# Task to execute if 'event' is odd
def odd_task(**kwargs):
    # ... existing code ...
    pass

odd_task_operator = PythonOperator(
    task_id='odd_task',
    python_callable=odd_task,
    dag=dag,
)

# Final task
def end_task(**kwargs):
    # ... existing code ...
    pass

end_task_operator = PythonOperator(
    task_id='end_task',
    python_callable=end_task,
    trigger_rule='one_success',
    dag=dag,
)

# Set the task dependencies
check_event_task >> [even_task_operator, odd_task_operator]
even_task_operator >> end_task_operator
odd_task_operator >> end_task_operator