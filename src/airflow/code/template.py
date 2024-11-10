import json
from datetime import datetime, timedelta

from airflow import DAG
{% for operator_class, operator_import in operator_imports.items() %}
from {{ operator_import }} import {{ operator_class }}
{% endfor %}
from airflow.utils.task_group import TaskGroup

# Import custom functions if any
{% if custom_functions %}
{% for func in custom_functions %}
from {{ func.module }} import {{ func.function }}
{% endfor %}
{% endif %}

default_args = {
    'owner': '{{ default_args.owner }}',
    'depends_on_past': {{ default_args.depends_on_past }},
    'email': {{ default_args.email }},
    'email_on_failure': {{ default_args.email_on_failure }},
    'email_on_retry': {{ default_args.email_on_retry }},
    'retries': {{ default_args.retries }},
    'retry_delay': timedelta(seconds={{ default_args.retry_delay }}),
}

with DAG(
        dag_id='{{ dag.dag_id }}',
        default_args=default_args,
        description='{{ dag.description }}',
        schedule_interval='{{ dag.schedule_interval }}',
        start_date=datetime.strptime('{{ dag.start_date }}', '%Y-%m-%d'),
        catchup={{ dag.catchup }},
        max_active_runs={{ dag.max_active_runs }},
        concurrency={{ dag.concurrency }},
        tags=[{% for tag in dag.tags_list %}'{{ tag }}',{% endfor %}],
) as dag:

    {% for group in task_groups %}
    with TaskGroup(group_id='{{ group.group_id }}', tooltip='{{ group.tooltip }}') as {{ group.group_id }}:
        {% for task in group.tasks %}
        {{ task.task_id }} = {{ task.operator_class }}(
            task_id='{{ task.task_id }}',
            {% for param, value in task.params.items() %}
        {{ param }}={{ value }},
        {% endfor %}
        {% if task.pool %}pool='{{ task.pool }}',{% endif %}
        {% if task.priority_weight %}priority_weight={{ task.priority_weight }},{% endif %}
        {% if task.retries %}retries={{ task.retries }},{% endif %}
        {% if task.retry_delay %}retry_delay=timedelta(seconds={{ task.retry_delay }}),{% endif %}
        {% if task.execution_timeout %}execution_timeout=timedelta(seconds={{ task.execution_timeout }}),{% endif %}
        {% if task.sla %}sla=timedelta(seconds={{ task.sla }}),{% endif %}
        {% if task.queue %}queue='{{ task.queue }}',{% endif %}
        {% if task.trigger_rule %}trigger_rule='{{ task.trigger_rule }}',{% endif %}
        {% if task.on_failure_callback %}on_failure_callback={{ task.on_failure_callback }},{% endif %}
        {% if task.on_success_callback %}on_success_callback={{ task.on_success_callback }},{% endif %}
        {% if task.resources %}
        resources={{ task.resources }},
        {% endif %}
        )
        {% endfor %}
        {% endfor %}

        {% for task in standalone_tasks %}
        {{ task.task_id }} = {{ task.operator_class }}(
            task_id='{{ task.task_id }}',
            {% for param, value in task.params.items() %}
        {{ param }}={{ value }},
        {% endfor %}
        {% if task.pool %}pool='{{ task.pool }}',{% endif %}
        {% if task.priority_weight %}priority_weight={{ task.priority_weight }},{% endif %}
        {% if task.retries %}retries={{ task.retries }},{% endif %}
        {% if task.retry_delay %}retry_delay=timedelta(seconds={{ task.retry_delay }}),{% endif %}
        {% if task.execution_timeout %}execution_timeout=timedelta(seconds={{ task.execution_timeout }}),{% endif %}
        {% if task.sla %}sla=timedelta(seconds={{ task.sla }}),{% endif %}
        {% if task.queue %}queue='{{ task.queue }}',{% endif %}
        {% if task.trigger_rule %}trigger_rule='{{ task.trigger_rule }}',{% endif %}
        {% if task.on_failure_callback %}on_failure_callback={{ task.on_failure_callback }},{% endif %}
        {% if task.on_success_callback %}on_success_callback={{ task.on_success_callback }},{% endif %}
        {% if task.resources %}
        resources={{ task.resources }},
        {% endif %}
        )
        {% endfor %}

        {% for dynamic_task in dynamic_tasks %}
        {{ dynamic_task.task_id }} = {{ dynamic_task.operator_class }}.expand(
            task_id='{{ dynamic_task.task_id }}',
            {% for param, value in dynamic_task.params.items() %}
        {{ param }}={{ value }},
        {% endfor %}
        )
        {% endfor %}

        {% for sensor in external_task_sensors %}
        {{ sensor.task_id }} = ExternalTaskSensor(
            task_id='{{ sensor.task_id }}',
            external_dag_id='{{ sensor.external_dag_id }}',
            external_task_id='{{ sensor.external_task_id }}',
            execution_delta=timedelta(seconds={{ sensor.execution_delta }}) if {{ sensor.execution_delta }} else None,
            execution_date_fn={{ sensor.execution_date_fn }} if {{ sensor.execution_date_fn }} else None,
            timeout={{ sensor.timeout }},
            poke_interval={{ sensor.poke_interval }},
            mode='{{ sensor.mode }}',
        )
        {% endfor %}

        # Setting up dependencies
        {% for dependency in dependencies %}
        {% if dependency.type == 'sequential' %}
        {{ dependency.upstream }} >> {{ dependency.downstream }}
        {% elif dependency.type == 'parallel' %}
        [{{ dependency.upstreams | join(', ') }}] >> {{ dependency.downstream }}
        {% elif dependency.type == 'conditional' %}
        {{ dependency.condition_task }} >> [{{ dependency.true_tasks | join(', ') }}]
        {% elif dependency.type == 'branch' %}
        {{ dependency.branch_task }} >> [{{ dependency.branch_options | join(', ') }}]
        {% elif dependency.type == 'join' %}
        [{{ dependency.upstreams | join(', ') }}] >> {{ dependency.downstream }}
        {% elif dependency.type == 'cross' %}
        {{ dependency.from_task }} >> {{ dependency.to_task }}
        {% endif %}
        {% endfor %}
