# File: dags/sqs_processing_dag.py

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import boto3
import json
import logging
from typing import Dict, List
import psycopg2
from psycopg2.extras import execute_batch

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# AWS SQS Configuration
AWS_ACCESS_KEY = 'your_access_key'
AWS_SECRET_KEY = 'your_secret_key'
AWS_REGION = 'us-east-1'
QUEUE_URL = 'your_sqs_queue_url'
BATCH_SIZE = 10  # Number of messages to process in one batch

# Database Configuration
DB_CONFIG = {
    'dbname': 'your_db',
    'user': 'your_user',
    'password': 'your_password',
    'host': 'your_host',
    'port': '5432'
}

# Create necessary tables for tracking metadata
def create_metadata_tables():
    """Create tables for tracking message processing metadata"""
    create_tables_sql = """
    -- Message Processing Tracking
    CREATE TABLE IF NOT EXISTS message_processing_metadata (
        message_id VARCHAR(100) PRIMARY KEY,
        queue_url VARCHAR(200),
        receipt_handle VARCHAR(1000),
        received_at TIMESTAMP,
        processed_at TIMESTAMP,
        status VARCHAR(20),
        retry_count INTEGER DEFAULT 0,
        processing_time_ms INTEGER,
        error_message TEXT,
        raw_message JSONB
    );

    -- Processing Batch Tracking
    CREATE TABLE IF NOT EXISTS processing_batch_metadata (
        batch_id SERIAL PRIMARY KEY,
        dag_run_id VARCHAR(100),
        start_time TIMESTAMP,
        end_time TIMESTAMP,
        total_messages INTEGER,
        successful_messages INTEGER,
        failed_messages INTEGER,
        batch_status VARCHAR(20),
        error_details JSONB
    );
    """

    with psycopg2.connect(**DB_CONFIG) as conn:
        with conn.cursor() as cur:
            cur.execute(create_tables_sql)
        conn.commit()

class SQSMessageProcessor:
    def __init__(self):
        """Initialize SQS client and database connection"""
        self.sqs_client = boto3.client(
            'sqs',
            aws_access_key_id=AWS_ACCESS_KEY,
            aws_secret_access_key=AWS_SECRET_KEY,
            region_name=AWS_REGION
        )
        self.conn = psycopg2.connect(**DB_CONFIG)

    def receive_messages(self) -> List[Dict]:
        """Receive messages from SQS queue"""
        try:
            response = self.sqs_client.receive_message(
                QueueUrl=QUEUE_URL,
                MaxNumberOfMessages=BATCH_SIZE,
                WaitTimeSeconds=20,
                AttributeNames=['All']
            )

            messages = response.get('Messages', [])
            logger.info(f"Received {len(messages)} messages from SQS")
            return messages

        except Exception as e:
            logger.error(f"Error receiving messages from SQS: {str(e)}")
            raise

    def process_message(self, message: Dict) -> bool:
        """Process individual message"""
        message_id = message['MessageId']
        receipt_handle = message['ReceiptHandle']
        body = json.loads(message['Body'])

        try:
            # Record message receipt
            self._record_message_receipt(message_id, message)

            # Process the message (implement your business logic here)
            # For example, transform data and load into database
            self._process_business_logic(body)

            # Mark message as processed
            self._update_message_status(message_id, 'PROCESSED')

            # Delete message from SQS
            self.sqs_client.delete_message(
                QueueUrl=QUEUE_URL,
                ReceiptHandle=receipt_handle
            )

            return True

        except Exception as e:
            logger.error(f"Error processing message {message_id}: {str(e)}")
            self._update_message_status(message_id, 'FAILED', str(e))
            return False

    def _record_message_receipt(self, message_id: str, message: Dict):
        """Record message metadata upon receipt"""
        sql = """
        INSERT INTO message_processing_metadata 
        (message_id, queue_url, receipt_handle, received_at, status, raw_message)
        VALUES (%s, %s, %s, %s, %s, %s)
        """

        with self.conn.cursor() as cur:
            cur.execute(sql, (
                message_id,
                QUEUE_URL,
                message['ReceiptHandle'],
                datetime.now(),
                'RECEIVED',
                json.dumps(message)
            ))
        self.conn.commit()

    def _update_message_status(self, message_id: str, status: str, error_message: str = None):
        """Update message processing status"""
        sql = """
        UPDATE message_processing_metadata
        SET status = %s,
            processed_at = %s,
            processing_time_ms = EXTRACT(EPOCH FROM (NOW() - received_at)) * 1000,
            error_message = %s
        WHERE message_id = %s
        """

        with self.conn.cursor() as cur:
            cur.execute(sql, (status, datetime.now(), error_message, message_id))
        self.conn.commit()

    def _process_business_logic(self, message_body: Dict):
        """
        Implement your business logic here
        For example: transform data and load into database
        """
        # Example business logic
        if 'data' not in message_body:
            raise ValueError("Message missing required 'data' field")

        # Process the data (example)
        processed_data = self._transform_data(message_body['data'])
        self._load_data(processed_data)

    def _transform_data(self, data: Dict) -> Dict:
        """Transform the message data"""
        # Add your transformation logic here
        return {
            'processed_timestamp': datetime.now().isoformat(),
            'source_data': data,
            # Add more transformed fields
        }

    def _load_data(self, data: Dict):
        """Load the processed data"""
        # Add your data loading logic here
        pass

    def record_batch_metadata(self, dag_run_id: str, total_messages: int,
                              successful: int, failed: int, errors: List[str]):
        """Record batch processing metadata"""
        sql = """
        INSERT INTO processing_batch_metadata
        (dag_run_id, start_time, end_time, total_messages, 
         successful_messages, failed_messages, batch_status, error_details)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """

        status = 'SUCCESS' if failed == 0 else 'PARTIAL_SUCCESS' if successful > 0 else 'FAILED'

        with self.conn.cursor() as cur:
            cur.execute(sql, (
                dag_run_id,
                datetime.now(),
                datetime.now(),
                total_messages,
                successful,
                failed,
                status,
                json.dumps({'errors': errors})
            ))
        self.conn.commit()

def process_sqs_messages(**context):
    """Main function to process SQS messages"""
    processor = SQSMessageProcessor()
    successful_count = 0
    failed_count = 0
    errors = []

    try:
        # Receive messages from SQS
        messages = processor.receive_messages()

        # Process each message
        for message in messages:
            if processor.process_message(message):
                successful_count += 1
            else:
                failed_count += 1
                errors.append(f"Failed to process message {message['MessageId']}")

        # Record batch metadata
        processor.record_batch_metadata(
            context['run_id'],
            len(messages),
            successful_count,
            failed_count,
            errors
        )

        return {
            'total_messages': len(messages),
            'successful': successful_count,
            'failed': failed_count
        }

    except Exception as e:
        logger.error(f"Error in batch processing: {str(e)}")
        raise

# Define the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'sqs_message_processing',
    default_args=default_args,
    description='Process messages from SQS queue',
    schedule_interval='*/10 * * * *',  # Run every 10 minutes
    catchup=False,
    max_active_runs=1
)

# Create metadata tables if they don't exist
create_tables_task = PythonOperator(
    task_id='create_metadata_tables',
    python_callable=create_metadata_tables,
    dag=dag,
)

# Process SQS messages
process_messages_task = PythonOperator(
    task_id='process_sqs_messages',
    python_callable=process_sqs_messages,
    provide_context=True,
    dag=dag,
)

create_tables_task >> process_messages_task