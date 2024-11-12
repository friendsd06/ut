# Use the official Python 3.9 image as a base
FROM python:3.9-slim

# Set environment variables
ENV AIRFLOW_HOME=/opt/airflow
ENV AIRFLOW_VERSION=2.9.2
ENV PYTHON_VERSION=3.9

# Install system dependencies
RUN apt-get update && apt-get install -y \
    build-essential \
    libssl-dev \
    libffi-dev \
    libblas-dev \
    liblapack-dev \
    libpq-dev \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Upgrade pip and install Airflow dependencies
RUN pip install --no-cache-dir --upgrade pip setuptools wheel

# Install Apache Airflow and necessary providers and Celery support
RUN pip install "apache-airflow[celery,postgres]==${AIRFLOW_VERSION}" \
    apache-airflow-providers-postgres \
    apache-airflow-providers-amazon \
    apache-airflow-providers-google \
    apache-airflow-providers-docker

# Create necessary directories
RUN mkdir -p ${AIRFLOW_HOME}/dags ${AIRFLOW_HOME}/logs ${AIRFLOW_HOME}/plugins

# Set the working directory
WORKDIR ${AIRFLOW_HOME}

# Expose the webserver port
EXPOSE 8080

# Set the entrypoint for Airflow
ENTRYPOINT ["airflow"]
