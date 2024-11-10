CREATE USER airflow WITH PASSWORD 'airflow';

-- Create Airflow database
CREATE DATABASE airflow;

-- Grant privileges
ALTER USER airflow WITH SUPERUSER;
GRANT ALL PRIVILEGES ON DATABASE airflow TO airflow;

init-scripts/01-init.sql
./init-scripts:/docker-entrypoint-initdb.d