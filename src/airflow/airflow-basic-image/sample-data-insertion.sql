-- sample_data_sqlite.sql

-- Insert sample DAG configurations
INSERT INTO DAG_CONFIGS (
    id,
    dag_id,
    description,
    owner,
    email_notifications,
    is_active,
    environment,
    created_by
) VALUES (
    'a1b2c3d4-e5f6-g7h8-i9j0-k1l2m3n4o5p6',
    'sales_etl_pipeline',
    'Daily sales data processing pipeline',
    'data_engineering',
    '["data_alerts@company.com", "etl_team@company.com"]',
    1,
    'prod',
    'john.doe'
);

INSERT INTO DAG_VERSIONS (
    id,
    dag_config_id,
    version,
    schedule_config,
    tags,
    is_current,
    deployed_by,
    deployment_notes
) VALUES (
    'b1b2c3d4-e5f6-g7h8-i9j0-k1l2m3n4o5p6',
    'a1b2c3d4-e5f6-g7h8-i9j0-k1l2m3n4o5p6',
    1,
    '{
        "schedule_interval": "@daily",
        "catchup": false,
        "max_active_runs": 1,
        "start_date": "2024-01-01",
        "concurrency": 1
    }',
    '["sales", "etl", "production"]',
    1,
    'john.doe',
    'Initial pipeline deployment'
);

-- Insert Tasks
INSERT INTO TASKS (
    id,
    dag_version_id,
    task_id,
    task_group,
    operator_class,
    operator_import,
    operator_config,
    task_order
) VALUES 
-- Extract Task
(
    'c1b2c3d4-e5f6-g7h8-i9j0-k1l2m3n4o5p6',
    'b1b2c3d4-e5f6-g7h8-i9j0-k1l2m3n4o5p6',
    'extract_sales_data',
    'data_extraction',
    'S3KeySensor',
    'airflow.providers.amazon.aws.sensors.s3',
    '{
        "bucket_name": "sales-data",
        "bucket_key": "daily/{{ ds }}/sales.csv",
        "aws_conn_id": "aws_default",
        "poke_interval": 300
    }',
    1
),
-- Transform Task
(
    'd1b2c3d4-e5f6-g7h8-i9j0-k1l2m3n4o5p6',
    'b1b2c3d4-e5f6-g7h8-i9j0-k1l2m3n4o5p6',
    'transform_sales_data',
    'data_processing',
    'PythonOperator',
    'airflow.operators.python',
    '{
        "python_callable": "transform_sales_data",
        "provide_context": true
    }',
    2
),
-- Load Task
(
    'e1b2c3d4-e5f6-g7h8-i9j0-k1l2m3n4o5p6',
    'b1b2c3d4-e5f6-g7h8-i9j0-k1l2m3n4o5p6',
    'load_to_warehouse',
    'data_loading',
    'SnowflakeOperator',
    'airflow.providers.snowflake.operators.snowflake',
    '{
        "sql": "COPY INTO sales_table FROM @sales_stage",
        "warehouse": "compute_wh",
        "database": "analytics"
    }',
    3
);

-- Insert Task Dependencies
INSERT INTO TASK_DEPENDENCIES (
    id,
    task_id,
    upstream_task_id,
    dependency_type
) VALUES 
(
    'f1b2c3d4-e5f6-g7h8-i9j0-k1l2m3n4o5p6',
    'd1b2c3d4-e5f6-g7h8-i9j0-k1l2m3n4o5p6',  -- transform_sales_data
    'c1b2c3d4-e5f6-g7h8-i9j0-k1l2m3n4o5p6',  -- extract_sales_data
    'sequential'
),
(
    'g1b2c3d4-e5f6-g7h8-i9j0-k1l2m3n4o5p6',
    'e1b2c3d4-e5f6-g7h8-i9j0-k1l2m3n4o5p6',  -- load_to_warehouse
    'd1b2c3d4-e5f6-g7h8-i9j0-k1l2m3n4o5p6',  -- transform_sales_data
    'sequential'
);

-- Insert Task Configurations
INSERT INTO TASK_CONFIGS (
    id,
    task_id,
    config_type,
    config_value
) VALUES 
-- Extract Task Retry Config
(
    'h1b2c3d4-e5f6-g7h8-i9j0-k1l2m3n4o5p6',
    'c1b2c3d4-e5f6-g7h8-i9j0-k1l2m3n4o5p6',
    'retry',
    '{
        "retries": 3,
        "retry_delay": 300,
        "exponential_backoff": true
    }'
),
-- Transform Task Resource Config
(
    'i1b2c3d4-e5f6-g7h8-i9j0-k1l2m3n4o5p6',
    'd1b2c3d4-e5f6-g7h8-i9j0-k1l2m3n4o5p6',
    'resources',
    '{
        "cpu_limit": "1",
        "memory_limit": "2Gi"
    }'
),
-- Load Task SLA Config
(
    'j1b2c3d4-e5f6-g7h8-i9j0-k1l2m3n4o5p6',
    'e1b2c3d4-e5f6-g7h8-i9j0-k1l2m3n4o5p6',
    'sla',
    '{
        "sla_time": 3600,
        "notification_channels": ["slack", "email"]
    }'
);