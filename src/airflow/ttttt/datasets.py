from airflow.datasets import Dataset

# Define Datasets with container paths
file1_dataset = Dataset("file:///opt/airflow/dataset-path/file1.csv")
file2_dataset = Dataset("file:///opt/airflow/dataset-path/file2.csv")
file3_dataset = Dataset("file:///opt/airflow/dataset-path/file3.csv")
