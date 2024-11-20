class DAGQueries:
    GET_DAG = """
        SELECT 
            PipelineID as dag_id,
            PipelineName as dag_name,
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
        FROM Pipeline
        WHERE PipelineID = %s;
    """

    GET_TASKS = """
        SELECT DISTINCT
            *
        FROM Tasks t
        JOIN Template temp 
            ON t.TemplateID = temp.TemplateID
        JOIN PipelineDatasetDependencies pdd 
            ON t.TaskID = pdd.task_id 
            AND t.TemplateID = pdd.template_id
        WHERE pdd.pipeline_id = %s
        ORDER BY t.task_priority_weight, t.TaskID;
    """

    GET_PIPELINE_DATASETS = """
        SELECT 
            d.DatasetID,
            d.DatasetName,
            pdd.processing_order
        FROM Datasets d
        JOIN PipelineDatasetDependencies pdd 
            ON d.DatasetID = pdd.source_dataset_id
        WHERE pdd.pipeline_id = %s
        ORDER BY pdd.processing_order;
    """

    GET_TASK_GROUPS = """
        SELECT *
        FROM taskgroups
        WHERE pipeline_id = %s
        ORDER BY group_id;
    """

    GET_DEPENDENCIES = """
        SELECT 
            pd1.task_id as upstream_task_id,
            pd2.task_id as downstream_task_id,
            pd1.processing_order,
            t1.TaskName as upstream_task_name,
            t2.TaskName as downstream_task_name
        FROM PipelineDatasetDependencies pd1
        JOIN PipelineDatasetDependencies pd2 
            ON pd1.target_dataset_id = pd2.source_dataset_id
            AND pd1.pipeline_id = pd2.pipeline_id
        JOIN Tasks t1 
            ON pd1.task_id = t1.TaskID
        JOIN Tasks t2 
            ON pd2.task_id = t2.TaskID
        WHERE pd1.pipeline_id = %s
            AND pd1.is_active = true
            AND pd2.is_active = true
        ORDER BY pd1.processing_order;
    """

    GET_ACTIVE_DAGS = """
        SELECT 
            pipelineid as dag_id,
            PipelineName as dag_name,
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
        FROM public.pipeline 
        WHERE is_active = true;
    """
