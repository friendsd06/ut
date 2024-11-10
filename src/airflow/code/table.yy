-- File: create_airflow_config.sql

-- Table to store DAG metadata
CREATE TABLE IF NOT EXISTS dags (
    dag_id TEXT PRIMARY KEY,
    description TEXT,
    schedule_interval TEXT,
    start_date TEXT, -- ISO format date string (YYYY-MM-DD)
    catchup INTEGER, -- 0 (False) or 1 (True)
    max_active_runs INTEGER,
    concurrency INTEGER,
    tags TEXT -- Comma-separated tags
);

-- Table to store task groups (optional)
CREATE TABLE IF NOT EXISTS task_groups (
    group_id TEXT PRIMARY KEY,
    dag_id TEXT,
    tooltip TEXT,
    FOREIGN KEY (dag_id) REFERENCES dags(dag_id)
);

-- Table to store tasks
CREATE TABLE IF NOT EXISTS tasks (
    task_id TEXT PRIMARY KEY,
    dag_id TEXT,
    group_id TEXT, -- Nullable, references task_groups(group_id)
    operator_class TEXT,
    operator_import TEXT,
    params TEXT, -- JSON string of operator parameters
    pool TEXT,
    priority_weight INTEGER,
    retries INTEGER,
    retry_delay INTEGER, -- Seconds
    execution_timeout INTEGER, -- Seconds
    sla INTEGER, -- Seconds
    queue TEXT,
    trigger_rule TEXT,
    on_failure_callback TEXT,
    on_success_callback TEXT,
    resources TEXT -- JSON string
);

-- Table to store dependencies
CREATE TABLE IF NOT EXISTS dependencies (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    dag_id TEXT,
    type TEXT, -- 'sequential', 'parallel', 'conditional', 'branch', 'join', 'cross'
    upstream TEXT, -- Single task_id or comma-separated task_ids
    downstream TEXT, -- Single task_id or comma-separated task_ids
    condition_task TEXT, -- For conditional dependencies
    true_tasks TEXT, -- Comma-separated task_ids for conditional
    branch_task TEXT, -- For branching
    branch_options TEXT, -- Comma-separated task_ids
    from_task TEXT, -- For 'cross' dependencies
    to_task TEXT,
    FOREIGN KEY (dag_id) REFERENCES dags(dag_id)
);

-- Table to store external task sensors (optional)
CREATE TABLE IF NOT EXISTS external_task_sensors (
    sensor_id INTEGER PRIMARY KEY AUTOINCREMENT,
    dag_id TEXT,
    task_id TEXT,
    external_dag_id TEXT,
    external_task_id TEXT,
    execution_delta INTEGER, -- Seconds
    execution_date_fn TEXT, -- Python expression as string
    timeout INTEGER,
    poke_interval INTEGER,
    mode TEXT,
    FOREIGN KEY (dag_id) REFERENCES dags(dag_id)
);

-- Table to store custom functions (optional)
CREATE TABLE IF NOT EXISTS custom_functions (
    function_id INTEGER PRIMARY KEY AUTOINCREMENT,
    dag_id TEXT,
    module TEXT,
    function TEXT,
    FOREIGN KEY (dag_id) REFERENCES dags(dag_id)
);

-- Table to store dynamic tasks (for dynamic task mapping, optional)
CREATE TABLE IF NOT EXISTS dynamic_tasks (
    dynamic_task_id INTEGER PRIMARY KEY AUTOINCREMENT,
    dag_id TEXT,
    task_id TEXT,
    operator_class TEXT,
    operator_import TEXT,
    params TEXT -- JSON string
);
