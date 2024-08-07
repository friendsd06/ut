from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from delta.tables import *
import json

# Initialize Spark Session with Delta Lake and S3 support
spark = SparkSession.builder \
    .appName("AdvancedDeltaLakeS3Internals") \
    .config("spark.jars.packages",
            "io.delta:delta-core_2.12:1.0.0," +
            "org.apache.hadoop:hadoop-aws:3.2.0") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.access.key", "YOUR_AWS_ACCESS_KEY") \
    .config("spark.hadoop.fs.s3a.secret.key", "YOUR_AWS_SECRET_KEY") \
    .config("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com") \
    .getOrCreate()

# Disable optimizations to see raw file structure
spark.conf.set("spark.databricks.delta.optimizeWrite.enabled", "false")
spark.conf.set("spark.databricks.delta.autoCompact.enabled", "false")
spark.conf.set("spark.sql.shuffle.partitions", "4")  # Set low for demonstration
spark.conf.set("spark.sql.files.maxRecordsPerFile", 25)  # Force creation of small files


# Define schema for customer orders (same as before)
schema = StructType([
    StructField("order_id", StringType(), False),
    StructField("customer_id", StringType(), False),
    StructField("order_date", DateType(), False),
    StructField("product_id", StringType(), False),
    StructField("quantity", IntegerType(), False),
    StructField("price", DoubleType(), False)
])

# Function to examine Delta table internals (adapted for S3)
def examine_delta_internals(path, operation=""):
    print(f"\n--- Delta Table Internals after {operation} ---")

    # List files
    print("Files in Delta table directory:")
    files = spark.sparkContext.textFile(f"{path}/[!_]*").collect()
    for file in files:
        print(file)

    # Show file count
    file_count = spark.sql(f"DESCRIBE DETAIL delta.`{path}`").select("numFiles").collect()[0][0]
    print(f"\nNumber of data files: {file_count}")

    # Examine Delta Log
    print("\nDelta Log contents:")
    log_files = spark.sparkContext.textFile(f"{path}/_delta_log/*.json").collect()
    for log in log_files:
        print(log)

    # Read and display the latest Delta log file
    latest_log_file = sorted(log_files)[-1]
    log_content = spark.sparkContext.textFile(latest_log_file).first()
    print("\nLatest Delta Log File Content (truncated):")
    log_json = json.loads(log_content)
    print(json.dumps({k: log_json[k] for k in list(log_json.keys())[:3]}, indent=2))  # Show first 3 keys

    # Show table history
    print("\nTable History:")
    spark.sql(f"DESCRIBE HISTORY delta.`{path}`").show(truncate=False)


    # S3 path for our Delta table
s3_delta_path = "s3a://your-bucket-name/advanced_delta_demo"

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

# Step 2: Append new data
print("\nStep 2: Append new data")
new_data = [
    ("O004", "C003", "2023-01-04", "P001", 1, 10.0),
    ("O005", "C002", "2023-01-05", "P002", 2, 20.0),
]
new_df = spark.createDataFrame(new_data, schema)
new_df.write.format("delta").mode("append").save(s3_delta_path)
examine_delta_internals(s3_delta_path, "append")

# Step 3: Update existing data
print("\nStep 3: Update existing data")
delta_table = DeltaTable.forPath(spark, s3_delta_path)
delta_table.update(
    condition = "order_id = 'O001'",
    set = { "quantity": lit(3), "price": lit(9.5) }
)
examine_delta_internals(s3_delta_path, "update")

# Step 4: Delete some data
print("\nStep 4: Delete some data")
delta_table.delete("order_id = 'O002'")
examine_delta_internals(s3_delta_path, "delete")

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

# Step 6: Time travel query
print("\nStep 6: Time travel query")
version_2_df = spark.read.format("delta").option("versionAsOf", 2).load(s3_delta_path)
print("Data as of version 2:")
version_2_df.show()

# Step 7: Optimize table
print("\nStep 7: Optimize table")
delta_table.optimize().executeCompaction()
examine_delta_internals(s3_delta_path, "optimize")

# Step 8: Vacuum old files
print("\nStep 8: Vacuum old files")
delta_table.vacuum(0)  # Set to 0 for demonstration; use higher values in production
examine_delta_internals(s3_delta_path, "vacuum")

# Final step: Read current state of the table
print("\nFinal step: Current state of the table")
final_df = spark.read.format("delta").load(s3_delta_path)
final_df.show()