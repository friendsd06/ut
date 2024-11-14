# setup_database.ps1

# Set the connection parameters
$host = "localhost"
$port = "5432"
$username = "postgres"    # Replace with your PostgreSQL username
$password = "your_password"  # Replace with your PostgreSQL password
$database = "airflow_dag_generator"

# Function to execute SQL commands
function Execute-SqlCommand {
    param (
        [string]$SqlCommand
)

# Write the SQL command to a temporary file
$tempFile = [IO.Path]::GetTempFileName()
Set-Content -Path $tempFile -Value $SqlCommand

# Set the environment variable for PGPASSWORD
$env:PGPASSWORD = $password

# Execute the psql command
Write-Host "Executing SQL command..."
$result = & 'psql' -h $host -p $port -U $username -d $database -f $tempFile

# Clean up
Remove-Item $tempFile
Remove-Variable -Name PGPASSWORD -Scope Global

return $result
}

# Create the database if it doesn't exist
Write-Host "Creating database '$database'..."
$env:PGPASSWORD = $password
                   & 'psql' -h $host -p $port -U $username -tc "SELECT 1 FROM pg_database WHERE datname = '$database';" | Out-Null
if ($LASTEXITCODE -ne 0) {
& 'psql' -h $host -p $port -U $username -c "CREATE DATABASE $database;"
}
# Remove PGPASSWORD after use
Remove-Variable -Name PGPASSWORD -Scope Global

# SQL commands to create tables and insert data
$sqlCommands = @"
                -- Create the 'dags' table
CREATE TABLE IF NOT EXISTS dags (
id VARCHAR PRIMARY KEY,
description TEXT,
schedule VARCHAR NOT NULL,
start_date TIMESTAMP NOT NULL,
tags JSONB,
settings JSONB,
owner VARCHAR DEFAULT 'airflow',
email JSONB
);

-- Create the 'operators' table
CREATE TABLE IF NOT EXISTS operators (
id SERIAL PRIMARY KEY,
name VARCHAR NOT NULL UNIQUE,
package VARCHAR NOT NULL,
class_name VARCHAR NOT NULL,
description TEXT
);

-- Create the 'tasks' table
CREATE TABLE IF NOT EXISTS tasks (
id VARCHAR PRIMARY KEY,
dag_id VARCHAR NOT NULL REFERENCES dags(id),
operator_id INTEGER NOT NULL REFERENCES operators(id),
config JSONB DEFAULT '{}'::JSONB,
group_name VARCHAR,
retries INTEGER DEFAULT 0,
retry_delay INTEGER DEFAULT 300,
timeout INTEGER,
queue VARCHAR,
pool VARCHAR,
trigger_rule VARCHAR DEFAULT 'all_success'
);

-- Create the 'dependencies' table
CREATE TABLE IF NOT EXISTS dependencies (
id SERIAL PRIMARY KEY,
dag_id VARCHAR NOT NULL REFERENCES dags(id),
upstream_task_id VARCHAR NOT NULL REFERENCES tasks(id),
downstream_task_id VARCHAR NOT NULL REFERENCES tasks(id),
dependency_type VARCHAR DEFAULT 'default',
dependency_conditions JSONB
);

-- Insert sample data into 'operators' table
INSERT INTO operators (name, package, class_name, description)
VALUES
('PythonOperator', 'airflow.operators.python', 'PythonOperator', 'Executes Python callables'),
('BashOperator', 'airflow.operators.bash', 'BashOperator', 'Executes bash commands'),
('EmailOperator', 'airflow.operators.email', 'EmailOperator', 'Sends emails'),
('DummyOperator', 'airflow.operators.dummy', 'DummyOperator', 'Does nothing'),
('HttpSensor', 'airflow.sensors.http_sensor', 'HttpSensor', 'Waits for an HTTP response')
ON CONFLICT (name) DO NOTHING;

-- Insert sample data into 'dags' table
INSERT INTO dags (id, description, schedule, start_date, tags, settings, owner, email)
VALUES
('etl_dag', 'ETL Data Pipeline DAG', '@daily', '2022-01-01 00:00:00', '["ETL", "pipeline"]', '{}'::JSONB, 'data_engineer', '["data_team@example.com"]'::JSONB),
('reporting_dag', 'Reporting DAG', '0 6 * * *', '2022-01-01 06:00:00', '["reporting"]', '{}'::JSONB, 'analyst', '["analyst_team@example.com"]'::JSONB),
('notification_dag', 'Notification DAG', '@hourly', '2022-01-01 00:00:00', '["notification"]', '{}'::JSONB, 'alert_system', '["alerts@example.com"]'::JSONB),
('quality_check_dag', 'Data Quality Checks', '@daily', '2022-01-01 02:00:00', '["quality", "checks"]', '{}'::JSONB, 'qa_team', '["qa_team@example.com"]'::JSONB),
('api_monitoring_dag', 'API Monitoring', '@hourly', '2022-01-01 00:00:00', '["api", "monitoring"]', '{}'::JSONB, 'devops', '["devops_team@example.com"]'::JSONB)
ON CONFLICT (id) DO NOTHING;

-- Insert sample data into 'tasks' table
                                   -- Tasks for 'etl_dag'
INSERT INTO tasks (id, dag_id, operator_id, config, retries, retry_delay)
VALUES
('extract_task', 'etl_dag', 1, '{"python_callable": "extract_data"}', 1, 300),
('transform_task', 'etl_dag', 1, '{"python_callable": "transform_data"}', 1, 300),
('load_task', 'etl_dag', 1, '{"python_callable": "load_data"}', 1, 300)
ON CONFLICT (id) DO NOTHING;

-- Task for 'reporting_dag'
INSERT INTO tasks (id, dag_id, operator_id, config)
VALUES
('generate_report', 'reporting_dag', 2, '{"bash_command": "generate_report.sh"}')
ON CONFLICT (id) DO NOTHING;

-- Task for 'notification_dag'
INSERT INTO tasks (id, dag_id, operator_id, config)
VALUES
('send_notification', 'notification_dag', 3, '{"to": "user@example.com", "subject": "Alert", "html_content": "An event has occurred."}')
ON CONFLICT (id) DO NOTHING;

-- Task for 'quality_check_dag'
INSERT INTO tasks (id, dag_id, operator_id, config)
VALUES
('run_quality_checks', 'quality_check_dag', 1, '{"python_callable": "run_checks"}')
ON CONFLICT (id) DO NOTHING;

-- Tasks for 'api_monitoring_dag'
INSERT INTO tasks (id, dag_id, operator_id, config)
VALUES
('check_api_status', 'api_monitoring_dag', 5, '{"endpoint": "https://api.example.com/health"}'),
('notify_devops', 'api_monitoring_dag', 3, '{"to": "devops@example.com", "subject": "API Down", "html_content": "The API is not responding."}')
ON CONFLICT (id) DO NOTHING;

-- Insert sample data into 'dependencies' table
-- Dependencies within 'etl_dag'
INSERT INTO dependencies (dag_id, upstream_task_id, downstream_task_id)
VALUES
('etl_dag', 'extract_task', 'transform_task'),
('etl_dag', 'transform_task', 'load_task'),
('etl_dag', 'extract_task', 'load_task')
ON CONFLICT DO NOTHING;

-- Dependencies within 'api_monitoring_dag'
INSERT INTO dependencies (dag_id, upstream_task_id, downstream_task_id)
VALUES
('api_monitoring_dag', 'check_api_status', 'notify_devops')
ON CONFLICT DO NOTHING;
"@

# Execute the SQL commands
Execute-SqlCommand -SqlCommand $sqlCommands

Write-Host "Database setup and data insertion complete."