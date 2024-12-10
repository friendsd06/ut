class DAGQueries:
    GET_DAG = """ SELECT * FROM Pipeline WHERE PipelineID = %s;
    """

    GET_TASKS = """ SELECT * FROM tasks WHERE pipelineid = %s; """

    GET_TASK_GROUPS = """ SELECT * FROM tasks WHERE pipelineid = %s ;
    """

    GET_DEPENDENCIES = """
        SELECT pipelineid, taskid, dependenttaskid FROM taskdependencies;
    """

    GET_ACTIVE_DAGS = """ SELECT * FROM pipeline WHERE is_active = true and pipelineid = 'loan_risk_analytics';
    """

    GET_TASK_DEPENDENCY = """SELECT pipelineid, taskid, dependenttaskid FROM taskdependencies  WHERE pipelineid = %s;"""

    GET_TASK_META_DATA = """ SELECT taskid,operator_type,dependencytype FROM tasks WHERE pipelineid = %s;
        """
