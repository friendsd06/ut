class DAGQueries:

    GET_DAG = """
        SELECT 
            PipelineID AS dag_id,
            PipelineName AS dag_name,
            schedule_interval,
            start_date,
            catchup,
            max_active_runs,
            concurrency,
            retries,
            retry_delay_minutes,
            dag_timeout_minutes,
            owner,
            email_on_failure,
            email_on_retry,
            notification_emails,
            dag_tags,
            default_args
        FROM 
            Pipeline
        WHERE 
            PipelineID = %s;
    """

    GET_TASKS = """
        SELECT * FROM tasks pipeline_id = %s;
    """

    GET_TASK_GROUPS = """
        SELECT 
            *
        FROM 
            taskgroups
        WHERE 
            pipeline_id = %s
        ORDER BY 
            group_id;
    """

    GET_DEPENDENCIES = """
        SELECT pipelineid, taskid, dependenttaskid FROM taskdependencies;
    """

    GET_ACTIVE_DAGS = """
        SELECT 
            pipelineid AS dag_id,
            PipelineName AS dag_name,
            slice,
            template,
            schedule_interval,
            start_date,
            catchup,
            max_active_runs,
            concurrency,
            retries,
            retry_delay_minutes,
            dag_timeout_minutes,
            owner,
            email_on_failure,
            email_on_retry,
            notification_emails,
            env_vars,
            dag_tags,
            default_args,
            is_active,
            created_at
        FROM 
            public.pipeline 
        WHERE 
            is_active = true;
    """
