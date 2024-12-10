class DAGQueries2:
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
     SELECT pipelineid, taskid, dependenttaskid FROM taskdependencies;
    """

    GET_ACTIVE_DAGS = """
        SELECT *
        FROM dag_metadata
        WHERE is_active = true
        ORDER BY dag_id;
    """