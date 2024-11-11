from airflow.operators.python_operator import PythonOperator
from datetime import timedelta


def enhanced_callable(python_callable, **context):
    try:

        validate_data(context)

        result = python_callable(**context)

        result = cleanup_data(result, context)

        return result
    except Exception as e:

        handle_errors(e, context)

        raise


send_report = PythonOperator(
    task_id="send_report",
    python_callable=enhanced_callable,
    op_kwargs={
        "python_callable": send_report_fn,
        **{"report_type": "daily", "email": "team@example.com"},
    },
)
