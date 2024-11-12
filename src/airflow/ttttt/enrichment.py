from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.datasets import Dataset
from datetime import datetime, timedelta

# Import the Datasets
from datasets import file1_dataset, file2_dataset, file3_dataset

def enrich_data():
    """
    Enrichment logic that runs after all ingestion tasks are complete.
    """
    print("Starting enrichment process...")
    try:
        import pandas as pd

        # Read the ingested CSV files
        df1 = pd.read_csv("/opt/airflow/dataset-path/file1.csv")
        df2 = pd.read_csv("/opt/airflow/dataset-path/file2.csv")
        df3 = pd.read_csv("/opt/airflow/dataset-path/file3.csv")

        # Example enrichment: Merging DataFrames on 'id'
        merged_df = df1.merge(df2, on='id').merge(df3, on='id')
        print("Merged DataFrame:")
        print(merged_df)

        # Save the enriched data
        enriched_path = "/opt/airflow/dataset-path/enriched_data.csv"
        merged_df.to_csv(enriched_path, index=False)
        print(f"Enrichment process completed successfully. Saved to {enriched_path}.")

    except Exception as e:
        print(f"Enrichment process failed: {e}")
        raise

default_args = {
    'owner': 'your_name',
    'depends_on_past': False,
    'retries': 0,  # No retries
}

with DAG(
    dag_id='enrichment_dag',
    default_args=default_args,
    description='Enrichment DAG that runs after all ingestion DAGs',
    schedule=[file1_dataset, file2_dataset, file3_dataset],  # Data-Aware Scheduling with Datasets
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=['enrichment'],
) as dag:

    start = DummyOperator(
        task_id='start'
    )

    enrich = PythonOperator(
        task_id='enrich_data',
        python_callable=enrich_data,
    )

    end = DummyOperator(
        task_id='end'
    )

    # Define task dependencies
    start >> enrich >> end
