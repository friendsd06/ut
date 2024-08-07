# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from delta.tables import *
import json

# COMMAND ----------

# Setup Spark Session
spark = SparkSession.builder.getOrCreate()

# Configure for S3 access
spark.conf.set("spark.databricks.delta.formatCheck.enabled", "false")
spark.conf.set("spark.sql.files.ignoreMissingFiles", "true")

# Disable auto compaction and optimizeWrite for this demonstration
spark.conf.set("spark.databricks.delta.autoCompact.enabled", "false")
spark.conf.set("spark.databricks.delta.optimizeWrite.enabled", "false")

# COMMAND ----------

# Define schema for our dataset
schema = StructType([
    StructField("id", IntegerType(), False),
    StructField("name", StringType(), True),
    StructField("age", IntegerType(), True),
    StructField("city", StringType(), True)
])

# COMMAND ----------

# S3 path for our Delta table
s3_delta_path = "/mnt/your-s3-mount/merge_internals_demo"

# COMMAND ----------

# Function to examine Delta table internals
def examine_delta_internals(path, operation=""):
    print(f"\n--- Delta Table Internals after {operation} ---")

    # List files
    print("Files in Delta table directory:")
    files = dbutils.fs.ls(path)
    for file in files:
        print(file.path)

    # Show file count
    file_count = spark.sql(f"DESCRIBE DETAIL delta.`{path}`").select("numFiles").collect()[0][0]
    print(f"\nNumber of data files: {file_count}")

    # Examine Delta Log
    print("\nDelta Log contents:")
    log_files = dbutils.fs.ls(f"{path}/_delta_log")
    for log in log_files:
        print(log.path)

    # Read and display the latest Delta log file
    latest_log_file = sorted(log_files, key=lambda f: f.name, reverse=True)[0]
    log_content = dbutils.fs.head(latest_log_file.path)
    print("\nLatest Delta Log File Content:")
    log_json = json.loads(log_content)
    print(json.dumps(log_json, indent=2))

    # Show table history
    print("\nTable History:")
    display(spark.sql(f"DESCRIBE HISTORY delta.`{path}`"))

# COMMAND ----------

# Step 1: Create initial dataset
print("Step 1: Creating initial dataset")
initial_data = [
    (1, "Alice", 30, "New York"),
    (2, "Bob", 35, "San Francisco"),
    (3, "Charlie", 40, "Los Angeles")
]
df = spark.createDataFrame(initial_data, schema)
df.write.format("delta").mode("overwrite").save(s3_delta_path)
examine_delta_internals(s3_delta_path, "initial write")

# COMMAND ----------

# Step 2: Prepare merge dataset
print("\nStep 2: Preparing merge dataset")
merge_data = [
    (2, "Bob", 36, "Chicago"),     # Update
    (3, "Charlie", 40, "Houston"), # Update
    (4, "David", 45, "Seattle")    # Insert
]
merge_df = spark.createDataFrame(merge_data, schema)
display(merge_df)

# COMMAND ----------

# Step 3: Perform merge operation
print("\nStep 3: Performing merge operation")
delta_table = DeltaTable.forPath(spark, s3_delta_path)

delta_table.alias("target").merge(
    merge_df.alias("source"),
    "target.id = source.id"
).whenMatchedUpdate(set =
{
    "age": "source.age",
    "city": "source.city"
}
).whenNotMatchedInsert(values =
{
    "id": "source.id",
    "name": "source.name",
    "age": "source.age",
    "city": "source.city"
}
).execute()

examine_delta_internals(s3_delta_path, "merge")

# COMMAND ----------

# Step 4: Examine final table state
print("\nStep 4: Examining final table state")
display(spark.read.format("delta").load(s3_delta_path))