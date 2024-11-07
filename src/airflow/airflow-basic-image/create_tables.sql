CREATE TABLE IF NOT EXISTS metadata_info (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL,
    description TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS dag_metadata (
    dag_id TEXT PRIMARY KEY,
    owner TEXT,
    start_date TIMESTAMP,
    end_date TIMESTAMP,
    schedule TEXT,
    status TEXT
);

CREATE TABLE IF NOT EXISTS task_metadata (
    task_id TEXT PRIMARY KEY,
    dag_id TEXT,
    start_time TIMESTAMP,
    end_time TIMESTAMP,
    status TEXT,
    FOREIGN KEY (dag_id) REFERENCES dag_metadata (dag_id)
);
