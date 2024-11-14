from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.S3_hook import S3Hook
from datetime import datetime
import os

# Define default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1),
}

# Set your MinIO sandbox URL and credentials
MINIO_ENDPOINT_URL = 'http://your-sandbox-url:9000'  # Replace with your MinIO endpoint URL
MINIO_ACCESS_KEY = 'YOUR_ACCESS_KEY'  # Replace with your MinIO access key
MINIO_SECRET_KEY = 'YOUR_SECRET_KEY'  # Replace with your MinIO secret key
MINIO_BUCKET_NAME = 'your-bucket-name'  # Replace with your bucket name
SOURCE_OBJECT_KEY = 'source-file.txt'  # Replace with your source object key
DESTINATION_OBJECT_KEY = 'destination-file.txt'  # Replace with your destination object key

# Initialize the DAG
with DAG(
        'minio_read_write_example',
        default_args=default_args,
        schedule_interval=None,
        catchup=False,
) as dag:

    def read_from_minio(**kwargs):
        """Read data from MinIO using S3Hook."""
        s3_hook = S3Hook(
            aws_conn_id=None,  # We won't use Airflow's connection
            aws_access_key_id=MINIO_ACCESS_KEY,
            aws_secret_access_key=MINIO_SECRET_KEY,
            endpoint_url=MINIO_ENDPOINT_URL,
            verify=False  # Set to False if using self-signed SSL certificates
        )
        bucket_name = MINIO_BUCKET_NAME
        key = SOURCE_OBJECT_KEY

        # Get the file object
        file_obj = s3_hook.get_key(key=key, bucket_name=bucket_name)
        if file_obj is None:
            raise ValueError(f"The object {key} does not exist in bucket {bucket_name}.")

        # Read the data
        data = file_obj.get()['Body'].read().decode('utf-8')
        print("Read data:", data)

        # Push data to XCom for use in the next task
        return data

    def process_data(**kwargs):
        """Process the data (e.g., convert to uppercase)."""
        ti = kwargs['ti']
        data = ti.xcom_pull(task_ids='read_from_minio_task')

        # Example transformation
        transformed_data = data.upper()
        print("Transformed data:", transformed_data)

        # Push transformed data to XCom
        return transformed_data

    def write_to_minio(**kwargs):
        """Write data back to MinIO."""
        ti = kwargs['ti']
        transformed_data = ti.xcom_pull(task_ids='process_data_task')

        s3_hook = S3Hook(
            aws_conn_id=None,
            aws_access_key_id=MINIO_ACCESS_KEY,
            aws_secret_access_key=MINIO_SECRET_KEY,
            endpoint_url=MINIO_ENDPOINT_URL,
            verify=False
        )
        bucket_name = MINIO_BUCKET_NAME
        key = DESTINATION_OBJECT_KEY

        # Write the data
        s3_hook.load_string(
            string_data=transformed_data,
            key=key,
            bucket_name=bucket_name,
            replace=True,
        )
        print(f"Successfully wrote data to {bucket_name}/{key}")

    # Define tasks
    read_from_minio_task = PythonOperator(
        task_id='read_from_minio_task',
        python_callable=read_from_minio,
        provide_context=True,
    )

    process_data_task = PythonOperator(
        task_id='process_data_task',
        python_callable=process_data,
        provide_context=True,
    )

    write_to_minio_task = PythonOperator(
        task_id='write_to_minio_task',
        python_callable=write_to_minio,
        provide_context=True,
    )

    # Set task dependencies
    read_from_minio_task >> process_data_task >> write_to_minio_task