INSERT INTO dags (id, description, schedule, start_date, tags, settings)
VALUES (
    'example_dag',
    'Example DAG with Python and Bash tasks',
    '@daily',
    '2024-01-01',
    'example,demo',
    '{"catchup": false, "max_active_runs": 1}'
);

INSERT INTO tasks (dag_id, id, type, config, group_id)
VALUES
    ('example_dag', 'hello_bash', 'BASH', '{"bash_command": "echo ''Hello World''"}', NULL),
    ('example_dag', 'python_task', 'PYTHON', '{"python_callable": "print_date", "op_kwargs": {"msg": "Current date is"}}', NULL);

INSERT INTO dependencies (dag_id, type, from_tasks, to_tasks)
VALUES ('example_dag', 'SEQUENTIAL', 'hello_bash', 'python_task');