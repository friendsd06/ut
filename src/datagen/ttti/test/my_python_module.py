# my_python_module.py
def my_python_callable(**kwargs):
    print("Executing my_python_callable")
    param1 = kwargs.get('param1')
    print(f"param1: {param1}")


# operators/branch_operator.py

def check_condition(**context):
    """
    Simple branch condition checker
    Returns task_id based on a condition
    """
    value = context['task_instance'].xcom_pull(key='value', task_ids='start_task')
    if value > 100:
        return 'high_value_task'
    else:
        return 'low_value_task'


def start_task(**context):
    """
    Start task that sets a value
    """
    value = 150  # You can modify this value to test different paths
    context['task_instance'].xcom_push(key='value', value=value)
    return value


def high_value_task(**context):
    """
    Task for high value processing
    """
    print("Processing High Value Path")
    return "High Value Processed"


def low_value_task(**context):
    """
    Task for low value processing
    """
    print("Processing Low Value Path")
    return "Low Value Processed"


def end_task(**context):
    """
    Final task after branching
    """
    print("Processing Complete")
    return "Done"
