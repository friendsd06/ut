import json
import sqlite3
from datetime import datetime, timedelta

from airflow import DAG
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.utils.task_group import TaskGroup
from airflow.models import Variable

# Define the path to your SQLite database
SQLITE_DB_PATH = '/usr/local/airflow/airflow.db'  # Adjust the path as needed

def get_dag_config(dag_id):
    """Fetch DAG configuration from the SQLite database."""
    conn = sqlite3.connect(SQLITE_DB_PATH)
    cursor = conn.cursor()

    # Fetch DAG metadata
    cursor.execute("SELECT * FROM dags WHERE dag_id = ?", (dag_id,))
    dag_row = cursor.fetchone()
    if not dag_row:
        conn.close()
        raise ValueError(f"DAG with id '{dag_id}' not found in the database.")

    dag_columns = [description[0] for description in cursor.description]
    dag_config = dict(zip(dag_columns, dag_row))

    # Fetch task groups
    cursor.execute("SELECT * FROM task_groups WHERE dag_id = ?", (dag_id,))
    groups = cursor.fetchall()
    group_configs = []
    group_columns = [description[0] for description in cursor.description]
    for group in groups:
        group_dict = dict(zip(group_columns, group))
        group_configs.append(group_dict)

    # Fetch tasks
    cursor.execute("SELECT * FROM tasks WHERE dag_id = ?", (dag_id,))
    tasks = cursor.fetchall()
    task_configs = []
    task_columns = [description[0] for description in cursor.description]
    for task in tasks:
        task_dict = dict(zip(task_columns, task))
        # Parse JSON fields
        task_dict['params'] = json.loads(task_dict['params']) if task_dict['params'] else {}
        task_dict['resources'] = json.loads(task_dict['resources']) if task_dict['resources'] else {}
        task_configs.append(task_dict)

    # Fetch dependencies
    cursor.execute("SELECT * FROM dependencies WHERE dag_id = ?", (dag_id,))
    dependencies = cursor.fetchall()
    dependency_configs = []
    dependency_columns = [description[0] for description in cursor.description]
    for dep in dependencies:
        dep_dict = dict(zip(dependency_columns, dep))
        dependency_configs.append(dep_dict)

    # Fetch external task sensors
    cursor.execute("SELECT * FROM external_task_sensors WHERE dag_id = ?", (dag_id,))
    sensors = cursor.fetchall()
    sensor_configs = []
    sensor_columns = [description[0] for description in cursor.description]
    for sensor in sensors:
        sensor_dict = dict(zip(sensor_columns, sensor))
        sensor_configs.append(sensor_dict)

    # Fetch custom functions
    cursor.execute("SELECT * FROM custom_functions WHERE dag_id = ?", (dag_id,))
    functions = cursor.fetchall()
    function_configs = []
    function_columns = [description[0] for description in cursor.description]
    for func in functions:
        func_dict = dict(zip(function_columns, func))
        function_configs.append(func_dict)

    # Fetch dynamic tasks
    cursor.execute("SELECT * FROM dynamic_tasks WHERE dag_id = ?", (dag_id,))
    dynamic_tasks = cursor.fetchall()
    dynamic_task_configs = []
    dynamic_task_columns = [description[0] for description in cursor.description]
    for d_task in dynamic_tasks:
        d_task_dict = dict(zip(dynamic_task_columns, d_task))
        d_task_dict['params'] = json.loads(d_task_dict['params']) if d_task_dict['params'] else {}
        dynamic_task_configs.append(d_task_dict)

    conn.close()

    return {
        'dag': dag_config,
        'task_groups': group_configs,
        'tasks': task_configs,
        'dependencies': dependency_configs,
        'external_task_sensors': sensor_configs,
        'custom_functions': function_configs,
        'dynamic_tasks': dynamic_task_configs
    }

def load_custom_functions(function_configs):
    """Import custom functions dynamically."""
    import importlib

    functions = {}
    for func in function_configs:
        module_name = func['module']
        function_name = func['function']
        module = importlib.import_module(module_name)
        functions[function_name] = getattr(module, function_name)
    return functions

# Define your DAG ID here
DAG_ID = 'example_dynamic_dag'

# Fetch the DAG configuration
config = get_dag_config(DAG_ID)

# Load custom functions
custom_functions = load_custom_functions(config['custom_functions'])

# Define default_args
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['alerts@example.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': config['dag'].get('retries', 1),
    'retry_delay': timedelta(minutes=config['dag'].get('retry_delay', 5)),
}

# Initialize the DAG
dag = DAG(
    dag_id=config['dag']['dag_id'],
    default_args=default_args,
    description=config['dag']['description'],
    schedule_interval=config['dag']['schedule_interval'],
    start_date=datetime.fromisoformat(config['dag']['start_date']),
    catchup=bool(config['dag']['catchup']),
    max_active_runs=config['dag'].get('max_active_runs', 1),
    concurrency=config['dag'].get('concurrency', 16),
    tags=[tag.strip() for tag in config['dag']['tags'].split(',')] if config['dag']['tags'] else [],
)

with dag:
    # Create Task Groups
    task_groups = {}
    for group in config['task_groups']:
        tg = TaskGroup(group_id=group['group_id'], tooltip=group['tooltip'])
        task_groups[group['group_id']] = tg

    # Create Tasks
    tasks = {}
    for task in config['tasks']:
        # Dynamically import the operator
        operator_module = task['operator_import']
        operator_class = task['operator_class']
        module = __import__(operator_module, fromlist=[operator_class])
        Operator = getattr(module, operator_class)

        # Prepare task parameters
        task_params = task['params'].copy()
        # Replace any placeholders in params if needed

        # Handle timedelta fields
        timedelta_fields = ['retry_delay', 'execution_timeout', 'sla']
        for field in timedelta_fields:
            if task.get(field):
                task_params[field] = timedelta(seconds=task[field])

        # Instantiate the operator
        if task['group_id']:
            with task_groups[task['group_id']]:
                tasks[task['task_id']] = Operator(
                    task_id=task['task_id'],
                    **task_params
                )
        else:
            tasks[task['task_id']] = Operator(
                task_id=task['task_id'],
                **task_params
            )

    # Create Dynamic Tasks (Task Mapping)
    for d_task in config['dynamic_tasks']:
        operator_module = d_task['operator_import']
        operator_class = d_task['operator_class']
        module = __import__(operator_module, fromlist=[operator_class])
        Operator = getattr(module, operator_class)

        task_params = d_task['params'].copy()

        # Handle placeholders or Jinja templates if necessary
        # For example, processing 'op_kwargs' with templated fields

        # Instantiate the dynamic task
        tasks[d_task['task_id']] = Operator(
            task_id=d_task['task_id'],
            **task_params
        ).expand()

    # Create External Task Sensors
    for sensor in config['external_task_sensors']:
        tasks[sensor['task_id']] = ExternalTaskSensor(
            task_id=sensor['task_id'],
            external_dag_id=sensor['external_dag_id'],
            external_task_id=sensor['external_task_id'],
            execution_delta=timedelta(seconds=sensor['execution_delta']) if sensor['execution_delta'] else None,
            execution_date_fn=eval(sensor['execution_date_fn']) if sensor['execution_date_fn'] else None,
            timeout=sensor['timeout'],
            poke_interval=sensor['poke_interval'],
            mode=sensor['mode'],
        )

    # Set Dependencies
    for dependency in config['dependencies']:
        dep_type = dependency['type']
        if dep_type == 'sequential':
            upstream = tasks[dependency['upstream']]
            downstream = tasks[dependency['downstream']]
            upstream >> downstream
        elif dep_type == 'parallel':
            upstream_tasks = [tasks[tid.strip()] for tid in dependency['upstreams'].split(',')]
            downstream = tasks[dependency['downstream']]
            upstream_tasks >> downstream
        elif dep_type == 'conditional':
            condition_task = tasks[dependency['condition_task']]
            true_tasks = [tasks[tid.strip()] for tid in dependency['true_tasks'].split(',')]
            condition_task >> true_tasks
        elif dep_type == 'branch':
            branch_task = tasks[dependency['branch_task']]
            branch_options = [tasks[tid.strip()] for tid in dependency['branch_options'].split(',')]
            branch_task >> branch_options
        elif dep_type == 'join':
            upstream_tasks = [tasks[tid.strip()] for tid in dependency['upstreams'].split(',')]
            downstream = tasks[dependency['downstream']]
            upstream_tasks >> downstream
        elif dep_type == 'cross':
            from_task = tasks[dependency['from_task']]
            to_task = tasks[dependency['to_task']]
            from_task >> to_task
        else:
            raise ValueError(f"Unknown dependency type: {dep_type}")
