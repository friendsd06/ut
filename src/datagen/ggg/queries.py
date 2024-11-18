class DAGQueries:
    GET_DAG = """
        SELECT *
        FROM dag_metadata
        WHERE dag_id = %s;
    """

    GET_TASKS = """
        SELECT *
        FROM task_configs
        WHERE dag_id = %s
        ORDER BY task_id;
    """

    GET_TASK_GROUPS = """
        SELECT *
        FROM task_groups
        WHERE dag_id = %s
        ORDER BY group_id;
    """

    GET_DEPENDENCIES = """
        SELECT 
            upstream_task_id,
            downstream_task_id,
            dependency_type
        FROM task_dependencies
        WHERE dag_id = %s
        AND is_active = true
        ORDER BY upstream_task_id, downstream_task_id;
    """

    GET_ACTIVE_DAGS = """
        SELECT dag_id
        FROM dag_metadata
        WHERE is_active = true
        ORDER BY dag_id;
    """