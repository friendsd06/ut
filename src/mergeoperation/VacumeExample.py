# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from delta.tables import *
import random
from datetime import datetime, timedelta

# COMMAND ----------

# Setup Spark Session (pre-configured in Databricks)
spark = SparkSession.builder.getOrCreate()

# Configure for S3 access and optimize for large datasets
spark.conf.set("spark.databricks.delta.formatCheck.enabled", "false")
spark.conf.set("spark.sql.files.ignoreMissingFiles", "true")
spark.conf.set("spark.sql.shuffle.partitions", "400")  # Adjust based on your cluster size
spark.conf.set("spark.default.parallelism", "400")    # Adjust based on your cluster size

# COMMAND ----------

# Define schema for large customer transaction dataset
schema = StructType([
    StructField("transaction_id", StringType(), False),
    StructField("customer_id", StringType(), False),
    StructField("transaction_date", TimestampType(), False),
    StructField("product_id", StringType(), False),
    StructField("quantity", IntegerType(), False),
    StructField("price", DoubleType(), False),
    StructField("store_id", StringType(), False),
    StructField("payment_method", StringType(), False)
])

# COMMAND ----------

# Function to generate large dataset
def generate_large_dataset(num_records):
    def generate_record():
        transaction_id = f"T{random.randint(1, 999999999):09d}"
        customer_id = f"C{random.randint(1, 1000000):07d}"
        transaction_date = datetime.now() - timedelta(days=random.randint(0, 365))
        product_id = f"P{random.randint(1, 10000):05d}"
        quantity = random.randint(1, 10)
        price = round(random.uniform(1.0, 1000.0), 2)
        store_id = f"S{random.randint(1, 1000):04d}"
        payment_method = random.choice(["Credit Card", "Debit Card", "Cash", "Mobile Payment"])

        return (transaction_id, customer_id, transaction_date, product_id, quantity, price, store_id, payment_method)

    return spark.createDataFrame([generate_record() for _ in range(num_records)], schema)

# COMMAND ----------

# S3 path for our large Delta table
s3_delta_path = "/mnt/your-s3-mount/large_delta_demo"

# Generate and write initial large dataset (10 million records)
print("Generating and writing initial dataset...")
large_df = generate_large_dataset(10_000_000)
large_df.write.format("delta").mode("overwrite").partitionBy("transaction_date").save(s3_delta_path)

print(f"Initial dataset written to {s3_delta_path}")
print(f"Record count: {spark.read.format('delta').load(s3_delta_path).count()}")

# COMMAND ----------

# Examine table details
def examine_table_details(path):
    print("\n--- Table Details ---")
    display(spark.sql(f"DESCRIBE DETAIL delta.`{path}`"))

    print("\n--- Table History ---")
    display(spark.sql(f"DESCRIBE HISTORY delta.`{path}`"))

examine_table_details(s3_delta_path)

# COMMAND ----------

# Perform a complex query
print("Performing complex query...")
query_result = spark.read.format("delta").load(s3_delta_path) \
    .groupBy("store_id", "payment_method") \
    .agg(
    count("transaction_id").alias("transaction_count"),
    sum("price").alias("total_sales"),
    avg("quantity").alias("avg_quantity")
) \
    .orderBy(desc("total_sales")) \
    .limit(10)

display(query_result)

# COMMAND ----------

# Append more data (5 million records)
print("Appending more data...")
additional_df = generate_large_dataset(5_000_000)
additional_df.write.format("delta").mode("append").partitionBy("transaction_date").save(s3_delta_path)

print(f"Additional data appended")
print(f"New record count: {spark.read.format('delta').load(s3_delta_path).count()}")

examine_table_details(s3_delta_path)

# COMMAND ----------

# Perform an update operation
print("Performing update operation...")
delta_table = DeltaTable.forPath(spark, s3_delta_path)
delta_table.update(
    condition = "payment_method = 'Cash'",
    set = { "payment_method": lit("Physical Cash") }
)

print("Update completed")
examine_table_details(s3_delta_path)

# COMMAND ----------

# Optimize the table
print("Optimizing table...")
delta_table.optimize().executeCompaction()
print("Optimization completed")

examine_table_details(s3_delta_path)

# COMMAND ----------

# Vacuum the table
print("Vacuuming table...")
delta_table.vacuum(168)  # Retention period of 7 days (168 hours)
print("Vacuum completed")

examine_table_details(s3_delta_path)

# COMMAND ----------

# Time travel query
print("Performing time travel query...")
version_1_df = spark.read.format("delta").option("versionAsOf", 1).load(s3_delta_path)
print(f"Record count at version 1: {version_1_df.count()}")

# COMMAND ----------

# Final query to demonstrate performance
print("Performing final complex query...")
final_query_result = spark.read.format("delta").load(s3_delta_path) \
    .groupBy("store_id", "transaction_date") \
    .agg(
    count("transaction_id").alias("transaction_count"),
    sum("price").alias("total_sales"),
    avg("quantity").alias("avg_quantity"),
    countDistinct("customer_id").alias("unique_customers")
) \
    .orderBy(desc("total_sales")) \
    .limit(20)

display(final_query_result)