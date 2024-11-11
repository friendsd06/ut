import os
import json
from datetime import datetime
from jinja2 import Environment, FileSystemLoader, Template
import black
from pathlib import Path
import logging


class DAGCodeGenerator:
    def __init__(self, template_path='templates', output_path='generated_dags'):
        self.template_path = os.path.abspath(template_path)
        self.output_path = os.path.abspath(output_path)

        os.makedirs(self.template_path, exist_ok=True)
        os.makedirs(self.output_path, exist_ok=True)
        os.makedirs(os.path.join(self.output_path, 'dags'), exist_ok=True)
        os.makedirs(os.path.join(self.output_path, 'operators'), exist_ok=True)

        self.logger = self.setup_logger()
        self.template = self.get_template()

    def setup_logger(self):
        logger = logging.getLogger('DAGCodeGenerator')
        logger.setLevel(logging.INFO)
        console_handler = logging.StreamHandler()
        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        console_handler.setFormatter(formatter)
        logger.addHandler(console_handler)
        return logger

    def get_template(self):
        return """
from airflow.operators.python_operator import PythonOperator
from datetime import timedelta

{% if task.pre_execute_hook or task.post_execute_hook or task.error_handler %}
def enhanced_callable(python_callable, **context):
    try:
        {% if task.pre_execute_hook %}
        {{ task.pre_execute_hook }}(context)
        {% endif %}

        result = python_callable(**context)

        {% if task.post_execute_hook %}
        result = {{ task.post_execute_hook }}(result, context)
        {% endif %}

        return result
    except Exception as e:
        {% if task.error_handler %}
        {{ task.error_handler }}(e, context)
        {% endif %}
        raise
{% endif %}

{{ task.task_id }} = PythonOperator(
    task_id='{{ task.task_id }}',
    {% if task.pre_execute_hook or task.post_execute_hook or task.error_handler %}
    python_callable=enhanced_callable,
    op_kwargs={
        'python_callable': {{ task.python_callable }},
        {% if task.op_kwargs %}
        **{{ task.op_kwargs }},
        {% endif %}
    },
    {% else %}
    python_callable={{ task.python_callable }},
    {% if task.op_kwargs %}
    op_kwargs={{ task.op_kwargs }},
    {% endif %}
    {% endif %}

    {% if task.retries is defined %}
    retries={{ task.retries }},
    {% if task.retry_delay %}
    retry_delay=timedelta(seconds={{ task.retry_delay }}),
    {% endif %}
    {% endif %}

    {% if task.pool %}
    pool='{{ task.pool }}',
    {% endif %}

    {% if task.priority_weight %}
    priority_weight={{ task.priority_weight }},
    {% endif %}

    {% if task.execution_timeout %}
    execution_timeout=timedelta(seconds={{ task.execution_timeout }}),
    {% endif %}

    {% if task.trigger_rule and task.trigger_rule != 'all_success' %}
    trigger_rule='{{ task.trigger_rule }}',
    {% endif %}

    {% if task.email_on_failure is defined %}
    email_on_failure={{ task.email_on_failure }},
    {% endif %}

    {% if task.email_on_retry is defined %}
    email_on_retry={{ task.email_on_retry }},
    {% endif %}

    {% if task.description %}
    doc_md='''{{ task.description }}''',
    {% endif %}
)
"""

    def generate_operator_code(self, task_config):
        try:
            template = Template(self.template)
            rendered_code = template.render(task=task_config)
            try:
                formatted_code = black.format_str(rendered_code, mode=black.FileMode())
                return formatted_code
            except Exception as e:
                self.logger.warning(f"Code formatting failed: {e}")
                return rendered_code
        except Exception as e:
            self.logger.error(f"Error generating operator code: {e}")
            raise

    def save_code_to_file(self, code, filename, subfolder=None):
        try:
            if subfolder:
                file_path = os.path.join(self.output_path, subfolder, filename)
            else:
                file_path = os.path.join(self.output_path, filename)

            os.makedirs(os.path.dirname(file_path), exist_ok=True)

            with open(file_path, 'w') as f:
                f.write(code)

            self.logger.info(f"Generated code saved to {file_path}")
            return file_path
        except Exception as e:
            self.logger.error(f"Error saving code to file: {e}")
            raise

    def generate_from_config(self, config_file):
        try:
            with open(config_file, 'r') as f:
                config = json.load(f)

            generated_files = []
            for task in config.get('tasks', []):
                if not task.get('task_id') or not task.get('python_callable'):
                    raise ValueError(f"Task missing required fields: {task}")

                operator_code = self.generate_operator_code(task)
                operator_file = f"{task['task_id']}_operator.py"
                operator_path = self.save_code_to_file(
                    operator_code,
                    operator_file,
                    'operators'
                )
                generated_files.append(operator_path)

            return generated_files
        except Exception as e:
            self.logger.error(f"Error generating DAG from config: {e}")
            raise


def main():
    # Example configuration
    example_config = {
        "tasks": [
            {
                "task_id": "process_data",
                "python_callable": "process_data_fn",
                "description": "Process daily data",
                "retries": 3,
                "retry_delay": 300,
                "pool": "data_pool",
                "priority_weight": 2
            },
            {
                "task_id": "send_report",
                "python_callable": "send_report_fn",
                "pre_execute_hook": "validate_data",
                "post_execute_hook": "cleanup_data",
                "error_handler": "handle_errors",
                "op_kwargs": {
                    "report_type": "daily",
                    "email": "team@example.com"
                }
            }
        ]
    }

    config_path = 'D:\\Airflow-DAG\\dag_generator\\example_config.json'
    with open(config_path, 'w') as f:
        json.dump(example_config, f, indent=2)

    generator = DAGCodeGenerator()

    try:
        generated_files = generator.generate_from_config(config_path)

        print("\nGenerated files:")
        for file_path in generated_files:
            print(f"\nFile: {file_path}")
            with open(file_path, 'r') as f:
                print("\nGenerated code:")
                print(f.read())

    except Exception as e:
        print(f"Error: {e}")


if __name__ == "__main__":
    main()