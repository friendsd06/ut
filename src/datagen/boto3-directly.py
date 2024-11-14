from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
import boto3

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
        """Read data from MinIO using boto3."""
        s3_client = boto3.client(
            's3',
            endpoint_url=MINIO_ENDPOINT_URL,
            aws_access_key_id=MINIO_ACCESS_KEY,
            aws_secret_access_key=MINIO_SECRET_KEY,
            verify=False  # Set to False if using self-signed SSL certificates
        )
        bucket_name = MINIO_BUCKET_NAME
        key = SOURCE_OBJECT_KEY

        try:
            response = s3_client.get_object(Bucket=bucket_name, Key=key)
            data = response['Body'].read().decode('utf-8')
            print("Read data:", data)
            return data
        except Exception as e:
            raise ValueError(f"Error reading object {key} from bucket {bucket_name}: {e}")

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

        s3_client = boto3.client(
            's3',
            endpoint_url=MINIO_ENDPOINT_URL,
            aws_access_key_id=MINIO_ACCESS_KEY,
            aws_secret_access_key=MINIO_SECRET_KEY,
            verify=False  # Set to False if using self-signed SSL certificates
        )
        bucket_name = MINIO_BUCKET_NAME
        key = DESTINATION_OBJECT_KEY

        try:
            s3_client.put_object(Bucket=bucket_name, Key=key, Body=transformed_data)
            print(f"Successfully wrote data to {bucket_name}/{key}")
        except Exception as e:
            raise ValueError(f"Error writing object {key} to bucket {bucket_name}: {e}")

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
