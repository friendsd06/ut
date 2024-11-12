# Using the trigger script
from trigger_dag import AirflowAPITrigger

# Initialize the trigger
trigger = AirflowAPITrigger(
    host="http://your-airflow-host:8080",
    username="your_username",
    password="your_password"
)

# Prepare configuration
conf = {
    "data_date": "2024-01-01",
    "parameters": {
        "source": "external_system",
        "process_type": "full_load",
        "email_notification": "user@example.com"
    }
}

# Trigger the DAG
result = trigger.trigger_dag(
    dag_id="example_external_trigger_dag",
    conf=conf
)

# Check the result
print(f"DAG Run ID: {result['dag_run_id']}")
print(f"State: {result['state']}")