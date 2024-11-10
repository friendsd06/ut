-- schema_sqlite.sql

-- Enable foreign key support
PRAGMA foreign_keys = ON;

-- Drop existing tables if they exist
DROP TABLE IF EXISTS TASK_CONFIGS;
DROP TABLE IF EXISTS TASK_DEPENDENCIES;
DROP TABLE IF EXISTS TASKS;
DROP TABLE IF EXISTS DAG_VERSIONS;
DROP TABLE IF EXISTS DAG_CONFIGS;

-- Create DAG_CONFIGS table
CREATE TABLE DAG_CONFIGS (
    id TEXT PRIMARY KEY,
    dag_id TEXT UNIQUE NOT NULL,
    description TEXT,
    owner TEXT NOT NULL,
    email_notifications TEXT NOT NULL, -- Store as JSON array string
    is_active INTEGER DEFAULT 1,  -- SQLite boolean (0/1)
    environment TEXT NOT NULL CHECK (environment IN ('dev', 'stage', 'prod')),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    created_by TEXT NOT NULL
);

-- Create DAG_VERSIONS table
CREATE TABLE DAG_VERSIONS (
    id TEXT PRIMARY KEY,
    dag_config_id TEXT NOT NULL,
    version INTEGER NOT NULL,
    schedule_config TEXT NOT NULL,  -- Store as JSON string
    tags TEXT DEFAULT '[]',  -- Store as JSON array string
    is_current INTEGER DEFAULT 0,  -- SQLite boolean (0/1)
    deployed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    deployed_by TEXT NOT NULL,
    deployment_notes TEXT,
    FOREIGN KEY (dag_config_id) REFERENCES DAG_CONFIGS(id),
    UNIQUE (dag_config_id, version)
);

-- Create TASKS table
CREATE TABLE TASKS (
    id TEXT PRIMARY KEY,
    dag_version_id TEXT NOT NULL,
    task_id TEXT NOT NULL,
    task_group TEXT,
    operator_class TEXT NOT NULL,
    operator_import TEXT NOT NULL,
    operator_config TEXT NOT NULL,  -- Store as JSON string
    task_order INTEGER NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (dag_version_id) REFERENCES DAG_VERSIONS(id),
    UNIQUE (dag_version_id, task_id)
);

-- Create TASK_DEPENDENCIES table
CREATE TABLE TASK_DEPENDENCIES (
    id TEXT PRIMARY KEY,
    task_id TEXT NOT NULL,
    upstream_task_id TEXT NOT NULL,
    dependency_type TEXT NOT NULL CHECK (dependency_type IN ('sequential', 'parallel', 'branch')),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (task_id) REFERENCES TASKS(id),
    FOREIGN KEY (upstream_task_id) REFERENCES TASKS(id),
    UNIQUE (task_id, upstream_task_id)
);

-- Create TASK_CONFIGS table
CREATE TABLE TASK_CONFIGS (
    id TEXT PRIMARY KEY,
    task_id TEXT NOT NULL,
    config_type TEXT NOT NULL CHECK (config_type IN ('retry', 'sla', 'resources', 'monitoring')),
    config_value TEXT NOT NULL,  -- Store as JSON string
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (task_id) REFERENCES TASKS(id)
);

-- Create indexes
CREATE INDEX idx_dag_configs_environment ON DAG_CONFIGS(environment) WHERE is_active = 1;
CREATE INDEX idx_dag_versions_current ON DAG_VERSIONS(dag_config_id, is_current);
CREATE INDEX idx_tasks_dag_version ON TASKS(dag_version_id, task_order);
CREATE INDEX idx_task_dependencies_lookup ON TASK_DEPENDENCIES(task_id, upstream_task_id);
CREATE INDEX idx_task_configs_type ON TASK_CONFIGS(task_id, config_type);

-- Create view for current DAG versions
CREATE VIEW current_dag_versions AS
SELECT 
    dc.dag_id,
    dc.description,
    dc.owner,
    dc.environment,
    dv.version,
    dv.schedule_config,
    dv.tags,
    dv.deployed_at,
    dv.deployed_by
FROM DAG_CONFIGS dc
JOIN DAG_VERSIONS dv ON dc.id = dv.dag_config_id
WHERE dc.is_active = 1 AND dv.is_current = 1;