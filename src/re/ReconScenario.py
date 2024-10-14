from pyspark.sql import SparkSession
import random

# Initialize Spark session
spark = SparkSession.builder \
    .appName("Loan Dataset Reconciliation") \
    .getOrCreate()

# Define sample loan data schema and generate sample data
columns = ["loan_id", "customer_id", "amount", "interest_rate", "term",
           "loan_type", "status", "disbursed_date", "due_date", "balance"]

data = [
    (i, random.randint(1000, 9999), random.uniform(1000, 50000), random.uniform(3.0, 15.0),
     random.choice([12, 24, 36, 48, 60]), random.choice(["personal", "auto", "mortgage", "education"]),
     random.choice(["active", "closed", "default"]), "2023-01-01", "2024-01-01", random.uniform(0, 50000))
    for i in range(1, 101)
]

# Create source DataFrame
source_df = spark.createDataFrame(data, columns)

# Save to S3 (replace `your-s3-bucket` with your actual bucket)
source_df.write.mode("overwrite").parquet("s3://your-s3-bucket/source_loan_data/")
