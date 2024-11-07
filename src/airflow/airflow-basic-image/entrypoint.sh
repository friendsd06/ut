#!/bin/bash
set -e

# Initialize Airflow database
echo "Initializing Airflow database..."
airflow db init

# Run SQL commands to create metadata tables
echo "Creating metadata tables..."
sqlite3 $AIRFLOW_HOME/airflow.db < /create_tables.sql

# Create an admin user if not exists
echo "Creating Airflow admin user..."
airflow users list | grep admin || airflow users create \
    --username admin \
    --firstname Admin \
    --lastname User \
    --role Admin \
    --email admin@example.com \
    --password admin

# Start Airflow scheduler in the background
echo "Starting Airflow scheduler..."
airflow scheduler &

# Start Airflow web server in the foreground
echo "Starting Airflow web server..."
exec airflow webserver