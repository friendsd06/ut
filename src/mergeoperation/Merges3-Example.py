# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from delta.tables import *
import json

# COMMAND ----------

# Setup Spark Session (pre-configured in Databricks)
spark = SparkSession.builder.getOrCreate()

# Configure for S3 access
spark.conf.set("spark.databricks.delta.formatCheck.enabled", "false")
spark.conf.set("spark.sql.files.ignoreMissingFiles", "true")

# Disable optimizations to see raw file structure
spark.conf.set("spark.databricks.delta.optimizeWrite.enabled", "false")
spark.conf.set("spark.databricks.delta.autoCompact.enabled", "false")
spark.conf.set("spark.sql.shuffle.partitions", "4")
spark.conf.set("spark.sql.files.maxRecordsPerFile", 25)

# COMMAND ----------

# Define schema for customer orders
schema = StructType([
    StructField("order_id", StringType(), False),
    StructField("customer_id", StringType(), False),
    StructField("order_date", DateType(), False),
    StructField("product_id", StringType(), False),
    StructField("quantity", IntegerType(), False),
    StructField("price", DoubleType(), False)
])

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
    print("\nLatest Delta Log File Content (truncated):")
    log_json = json.loads(log_content)
    print(json.dumps({k: log_json[k] for k in list(log_json.keys())[:3]}, indent=2))

    # Show table history
    print("\nTable History:")
    display(spark.sql(f"DESCRIBE HISTORY delta.`{path}`"))

# COMMAND ----------

# S3 path for our Delta table (using Databricks mount point)
s3_delta_path = "/mnt/your-s3-mount/advanced_delta_demo"

# COMMAND ----------

# Step 1: Initial data write
print("\nStep 1: Initial data write")
initial_data = [
    ("O001", "C001", "2023-01-01", "P001", 2, 10.0),
    ("O002", "C002", "2023-01-02", "P002", 1, 20.0),
    ("O003", "C001", "2023-01-03", "P003", 3, 15.0),
]
df = spark.createDataFrame(initial_data, schema)
df.write.format("delta").mode("overwrite").save(s3_delta_path)
examine_delta_internals(s3_delta_path, "initial write")

# COMMAND ----------

# Step 2: Append new data
print("\nStep 2: Append new data")
new_data = [
    ("O004", "C003", "2023-01-04", "P001", 1, 10.0),
    ("O005", "C002", "2023-01-05", "P002", 2, 20.0),
]
new_df = spark.createDataFrame(new_data, schema)
new_df.write.format("delta").mode("append").save(s3_delta_path)
examine_delta_internals(s3_delta_path, "append")

# COMMAND ----------

# Step 3: Update existing data
print("\nStep 3: Update existing data")
delta_table = DeltaTable.forPath(spark, s3_delta_path)
delta_table.update(
    condition = "order_id = 'O001'",
    set = { "quantity": lit(3), "price": lit(9.5) }
)
examine_delta_internals(s3_delta_path, "update")

# COMMAND ----------

# Step 4: Delete some data
print("\nStep 4: Delete some data")
delta_table.delete("order_id = 'O002'")
examine_delta_internals(s3_delta_path, "delete")

# COMMAND ----------

# Step 5: Merge operation
print("\nStep 5: Merge operation")
merge_data = [
    ("O003", "C001", "2023-01-03", "P003", 4, 14.5),  # Update
    ("O006", "C003", "2023-01-06", "P002", 2, 20.0),  # Insert
]
merge_df = spark.createDataFrame(merge_data, schema)
delta_table.alias("orders").merge(
    merge_df.alias("updates"),
    "orders.order_id = updates.order_id"
).whenMatchedUpdate(set = {
    "quantity": "updates.quantity",
    "price": "updates.price"
}).whenNotMatchedInsert(values = {
    "order_id": "updates.order_id",
    "customer_id": "updates.customer_id",
    "order_date": "updates.order_date",
    "product_id": "updates.product_id",
    "quantity": "updates.quantity",
    "price": "updates.price"
}).execute()
examine_delta_internals(s3_delta_path, "merge")

# COMMAND ----------

# Step 6: Time travel query
print("\nStep 6: Time travel query")
version_2_df = spark.read.format("delta").option("versionAsOf", 2).load(s3_delta_path)
print("Data as of version 2:")
display(version_2_df)

# COMMAND ----------

# Step 7: Optimize table
print("\nStep 7: Optimize table")
delta_table.optimize().executeCompaction()
examine_delta_internals(s3_delta_path, "optimize")

# COMMAND ----------

# Step 8: Vacuum old files
print("\nStep 8: Vacuum old files")
delta_table.vacuum(0)  # Set to 0 for demonstration; use higher values in production
examine_delta_internals(s3_delta_path, "vacuum")

# COMMAND ----------

# Final step: Read current state of the table
print("\nFinal step: Current state of the table")
final_df = spark.read.format("delta").load(s3_delta_path)
display(final_df)