{
    "task_id": "data_processing",
    "operator_type": "bash",
    "bash_command": "python /scripts/process.py",
    "env_vars": {
        "DATA_PATH": "/data",
        "CONFIG_FILE": "config.json"
    },
    "cwd": "/scripts",
    "retries": 3,
    "retry_exponential_backoff": True,
    "pre_execute": """
        print("Setting up environment")
        os.makedirs('/tmp/data', exist_ok=True)
    """,
    "post_execute": """
        print("Cleanup")
        os.remove('/tmp/data/temp.txt')
    """
}