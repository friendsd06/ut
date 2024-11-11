CREATE TABLE IF NOT EXISTS dags (
    id TEXT PRIMARY KEY,
    description TEXT,
    schedule TEXT NOT NULL,
    start_date TEXT NOT NULL,
    tags TEXT,
    settings TEXT
);

CREATE TABLE IF NOT EXISTS tasks (
    dag_id TEXT,
    id TEXT,
    type TEXT NOT NULL,
    config TEXT,
    group_id TEXT,
    FOREIGN KEY (dag_id) REFERENCES dags(id)
);

CREATE TABLE IF NOT EXISTS dependencies (
    dag_id TEXT,
    type TEXT,
    from_tasks TEXT,
    to_tasks TEXT,
    FOREIGN KEY (dag_id) REFERENCES dags(id)
);