# dags/datasets.py

from airflow.datasets import Dataset

# Define a Dataset for each ingestion job with Windows file paths
file1_dataset = Dataset("file:///D:/airflow-docker/dataset-path/file1.csv")
file2_dataset = Dataset("file:///D:/airflow-docker/dataset-path/file2.csv")
file3_dataset = Dataset("file:///D:/airflow-docker/dataset-path/file3.csv")
