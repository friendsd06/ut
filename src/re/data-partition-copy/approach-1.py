# Complete PySpark Script: Writing Data and Copying Partitions Using Three Approaches

# 1. Import Necessary Libraries
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import boto3
from botocore.exceptions import ClientError

# 2. Initialize Spark Session
spark = SparkSession.builder.appName("PartitionCopyExample").getOrCreate()

# 3. Define S3 Bucket and Data Path
s3_bucket = "your-s3-bucket-name"
data_path = f"s3a://{s3_bucket}/data/path/to/"

# ============================
# Step 1: Write Sample Data to partition=23232
# ============================

# 4. Create Sample DataFrame
data = [
    (1, "Alice", "2024-05-23"),
    (2, "Bob", "2024-05-23"),
    (3, "Charlie", "2024-05-23"),
    (4, "David", "2024-05-23"),
    (5, "Eve", "2024-05-23")
]

columns = ["id", "name", "date"]

df = spark.createDataFrame(data, schema=columns)

# 5. Add Partition Column
df = df.withColumn("partition", col("date").substr(6, 5))  # Extracting '05-23' as partition (e.g., '0523')
df = df.withColumn("partition", col("partition").cast("int"))
df = df.withColumn("partition", col("partition") * 1000 + col("id"))  # Creating unique partition values

# 6. Filter for partition=23232
df_partition = df.filter(col("partition") == 23232)

# 7. Write Data to `partition=23232`
partition_path = f"{data_path}partition=23232"

df_partition.write.mode("overwrite").parquet(partition_path)

print(f"Sample data written to {partition_path}")

# ============================
# Approach 1: Using dbutils.fs.cp with S3A Paths
# ============================

print("\n--- Approach 1: Using dbutils.fs.cp ---")

# 8. Define Source and Destination Paths
source_partition_path = f"s3a://{s3_bucket}/data/path/to/partition=23232"
destination_partition_path_cp = f"s3a://{s3_bucket}/data/path/to/partition=5765"

# 9. Copy the Partition Directory Recursively
try:
    dbutils.fs.cp(source_partition_path, destination_partition_path_cp, recurse=True)
    print(f"Successfully copied from {source_partition_path} to {destination_partition_path_cp}.")
except Exception as e:
    print(f"Error during Approach 1 copy operation: {e}")

# ============================
# Approach 2: Leveraging Spark DataFrame Operations
# ============================

print("\n--- Approach 2: Using Spark DataFrame Operations ---")

# 10. Define Destination Path for Approach 2
destination_partition_path_df = f"s3a://{s3_bucket}/data/path/to/partition=5765_df"

# 11. Read Data from Source Partition
try:
    df_source = spark.read.parquet(source_partition_path)
    print(f"Data read successfully from {source_partition_path}.")
except Exception as e:
    print(f"Error reading source partition in Approach 2: {e}")
    df_source = None

# 12. Write Data to Destination Partition
if df_source:
    try:
        df_source.write.mode("overwrite").parquet(destination_partition_path_df)
        print(f"Data successfully written to {destination_partition_path_df}.")
    except Exception as e:
        print(f"Error writing to destination partition in Approach 2: {e}")

# ============================
# Approach 3: Utilizing AWS SDK (boto3) for S3 Operations
# ============================

print("\n--- Approach 3: Using AWS SDK (boto3) ---")

# 13. Initialize S3 Client
s3_client = boto3.client('s3')  # Assumes IAM roles are properly configured

# 14. Define Prefixes
source_prefix = "data/path/to/partition=23232/"
destination_prefix_boto = "data/path/to/partition=5765_boto/"

# 15. List Objects in Source Partition
try:
    paginator = s3_client.get_paginator('list_objects_v2')
    pages = paginator.paginate(Bucket=s3_bucket, Prefix=source_prefix)

    objects_to_copy = []
    for page in pages:
        if 'Contents' in page:
            for obj in page['Contents']:
                objects_to_copy.append(obj['Key'])

    print(f"Found {len(objects_to_copy)} objects to copy in Approach 3.")
except ClientError as e:
    print(f"Error listing objects in Approach 3: {e}")
    objects_to_copy = []

# 16. Copy Objects to Destination Partition
for obj_key in objects_to_copy:
    # Define the new key by replacing the source prefix with the destination prefix
    new_key = obj_key.replace(source_prefix, destination_prefix_boto, 1)

    try:
        copy_source = {
            'Bucket': s3_bucket,
            'Key': obj_key
        }
        s3_client.copy(copy_source, s3_bucket, new_key)
        print(f"Copied {obj_key} to {new_key}")
    except ClientError as e:
        print(f"Error copying {obj_key} to {new_key} in Approach 3: {e}")

# ============================
# Verification
# ============================

print("\n--- Verification ---")

# 17. Define Destination Paths for Verification
destination_partitions = [
    f"s3a://{s3_bucket}/data/path/to/partition=5765",
    f"s3a://{s3_bucket}/data/path/to/partition=5765_df",
    f"s3a://{s3_bucket}/data/path/to/partition=5765_boto"
]

for dest_path in destination_partitions:
    print(f"\nVerifying data in {dest_path}:")
    try:
        # List Files
        files = dbutils.fs.ls(dest_path)
        print(f"Files in {dest_path}:")
        for file in files:
            print(f"- {file.path}")

        # Read and Display Data
        df_verification = spark.read.parquet(dest_path)
        df_verification.show(truncate=False)
    except Exception as e:
        print(f"Error verifying {dest_path}: {e}")
