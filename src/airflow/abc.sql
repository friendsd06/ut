-- File: init-scripts/01-init.sql

-- Create airflow user
CREATE USER airflow WITH PASSWORD 'airflow';

-- Create airflow database
CREATE DATABASE airflow OWNER airflow;

-- Connect to the airflow database
\c airflow

-- Grant all privileges on all tables in airflow database to airflow user
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO airflow;
GRANT ALL PRIVILEGES ON SCHEMA public TO airflow;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO airflow;

init-scripts/01-init.sql
./init-scripts:/docker-entrypoint-initdb.d