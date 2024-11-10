# Use an official Python runtime as a parent image
FROM python:3.8-slim

# Set environment variables
ENV AIRFLOW_HOME=/usr/local/airflow
ENV AIRFLOW__CORE__EXECUTOR=SequentialExecutor
ENV AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=sqlite:////usr/local/airflow/airflow.db
ENV AIRFLOW__CORE__LOAD_EXAMPLES=False

# Install system dependencies, including sqlite3
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        build-essential \
        libssl-dev \
        libffi-dev \
        python3-dev \
        sqlite3 \
        && \
    rm -rf /var/lib/apt/lists/*

# Set Airflow version and constraints URL
ENV AIRFLOW_VERSION=2.5.0
ENV PYTHON_VERSION=3.8
ENV CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"

# Install Apache Airflow with constraints to ensure compatible dependencies
RUN pip install "apache-airflow==${AIRFLOW_VERSION}" --constraint "${CONSTRAINT_URL}"

# Explicitly install Flask-Session to resolve ModuleNotFoundError
RUN pip install Flask-Session==0.4.0

# Create necessary Airflow directories
RUN mkdir -p $AIRFLOW_HOME/dags $AIRFLOW_HOME/logs $AIRFLOW_HOME/plugins

# Copy entrypoint script and SQL schema file into the container
COPY entrypoint.sh /entrypoint.sh
COPY create_tables.sql /create_tables.sql
COPY schema-creation.sql /schema-creation.sql
COPY sample-data-insertion.sql /sample-data-insertion.sql
COPY dags/ /usr/local/airflow/dags/

# Make the entrypoint script executable
RUN chmod +x /entrypoint.sh

# Expose Airflow web server port
EXPOSE 8080

# Define the entrypoint
ENTRYPOINT ["/entrypoint.sh"]