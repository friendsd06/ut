# Use the official Python 3.9 image as a base
FROM python:3.9-slim

# Set environment variables
ENV AIRFLOW_HOME=/opt/airflow
ENV AIRFLOW_VERSION=2.9.2
ENV PYTHON_VERSION=3.9

# Install system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
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

# Install Apache Airflow and necessary providers with constraints
RUN pip install --no-cache-dir \
    "apache-airflow[celery,postgres]==${AIRFLOW_VERSION}" \
    apache-airflow-providers-postgres \
    apache-airflow-providers-amazon \
    apache-airflow-providers-google \
    apache-airflow-providers-docker \
    --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"

# Create necessary directories
RUN mkdir -p ${AIRFLOW_HOME}/dags ${AIRFLOW_HOME}/logs ${AIRFLOW_HOME}/plugins

# Create an airflow user
RUN useradd -ms /bin/bash airflow

# Change ownership to airflow user
RUN chown -R airflow: ${AIRFLOW_HOME}

# Switch to airflow user
USER airflow

# Set the working directory
WORKDIR ${AIRFLOW_HOME}

# Expose the webserver port
EXPOSE 8080

# Set the entrypoint for Airflow
ENTRYPOINT ["airflow"]
CMD ["webserver"]


# Initialize the Airflow database
docker run --rm \
    -e AIRFLOW__CORE__LOAD_EXAMPLES=False \
    -v airflow_data:/opt/airflow/airflow.db \
    my-airflow-image \
    airflow db init


#!/bin/bash
airflow db init
exec airflow "$@"


# Copy the entrypoint script
COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

# Set the entrypoint
ENTRYPOINT ["/entrypoint.sh"]

docker run --rm --entrypoint airflow my-airflow-image airflow db init
