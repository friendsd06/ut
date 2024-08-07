# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from delta.tables import *
import random
from datetime import datetime, timedelta
import time

# COMMAND ----------

# Function to create Spark session with specific Delta configurations
def create_spark_session(optimized=True):
    builder = SparkSession.builder.appName("DeltaOptimizationImpact")

    if optimized:
        builder = builder \
            .config("spark.databricks.delta.optimizeWrite.enabled", "true") \
            .config("spark.databricks.delta.autoCompact.enabled", "true") \
            .config("spark.databricks.delta.autoCompact.minNumFiles", "50") \
            .config("spark.databricks.delta.optimizeWrite.binSize", "128m") \
            .config("spark.delta.snapshotPartitions", "50") \
            .config("spark.databricks.delta.properties.defaults.enableChangeDataFeed", "true") \
            .config("spark.databricks.delta.schema.autoMerge.enabled", "true")
    else:
        builder = builder \
            .config("spark.databricks.delta.optimizeWrite.enabled", "false") \
            .config("spark.databricks.delta.autoCompact.enabled", "false") \
            .config("spark.databricks.delta.properties.defaults.enableChangeDataFeed", "false") \
            .config("spark.databricks.delta.schema.autoMerge.enabled", "false")

    return builder.getOrCreate()

# COMMAND ----------

# Define schema and data generation function
schema = StructType([
    StructField("id", IntegerType(), False),
    StructField("name", StringType(), True),
    StructField("department", StringType(), True),
    StructField("salary", DoubleType(), True),
    StructField("hire_date", DateType(), True)
])

def generate_data(num_records):
    def random_date():
        start_date = datetime(2010, 1, 1)
        end_date = datetime.now()
        return start_date + timedelta(days=random.randint(0, (end_date - start_date).days))

    data = [(i,
             f"Employee_{i}",
             random.choice(["HR", "IT", "Finance", "Marketing", "Sales"]),
             round(random.uniform(30000, 150000), 2),
             random_date().date())
            for i in range(1, num_records + 1)]
    return spark.createDataFrame(data, schema)

# COMMAND ----------

# Function to perform Delta operations and measure time
def perform_delta_operations(spark, s3_path, optimized):
    print(f"\n{'Optimized' if optimized else 'Non-Optimized'} Delta Operations:")

    # Initial write
    start_time = time.time()
    initial_df = generate_data(100000)
    initial_df.write.format("delta").mode("overwrite").partitionBy("department").save(s3_path)
    print(f"Initial write time: {time.time() - start_time:.2f} seconds")

    # Read operation
    start_time = time.time()
    read_df = spark.read.format("delta").load(s3_path)
    read_count = read_df.count()
    print(f"Read operation time: {time.time() - start_time:.2f} seconds")

    # Append operation
    start_time = time.time()
    append_df = generate_data(50000)
    append_df.write.format("delta").mode("append").save(s3_path)
    print(f"Append operation time: {time.time() - start_time:.2f} seconds")

    # Merge operation
    start_time = time.time()
    merge_df = generate_data(10000)
    delta_table = DeltaTable.forPath(spark, s3_path)
    delta_table.alias("target").merge(
        merge_df.alias("source"),
        "target.id = source.id"
    ).whenMatchedUpdate(set =
    {
        "salary": "source.salary",
        "department": "source.department"
    }
    ).whenNotMatchedInsert(values =
    {
        "id": "source.id",
        "name": "source.name",
        "department": "source.department",
        "salary": "source.salary",
        "hire_date": "source.hire_date"
    }
    ).execute()
    print(f"Merge operation time: {time.time() - start_time:.2f} seconds")

    # Complex query
    start_time = time.time()
    query_result = spark.read.format("delta").load(s3_path) \
        .groupBy("department") \
        .agg(
        count("id").alias("employee_count"),
        avg("salary").alias("avg_salary"),
        min("hire_date").alias("earliest_hire"),
        max("hire_date").alias("latest_hire")
    ) \
        .orderBy(desc("avg_salary"))
    query_result.collect()
    print(f"Complex query time: {time.time() - start_time:.2f} seconds")

    # Table details
    print("\nTable Details:")
    display(spark.sql(f"DESCRIBE DETAIL delta.`{s3_path}`"))

# COMMAND ----------

# S3 paths for our Delta tables
s3_optimized_path = "/mnt/your-s3-mount/delta_optimized_demo"
s3_non_optimized_path = "/mnt/your-s3-mount/delta_non_optimized_demo"

# Run optimized operations
spark_optimized = create_spark_session(optimized=True)
perform_delta_operations(spark_optimized, s3_optimized_path, optimized=True)

# Run non-optimized operations
spark_non_optimized = create_spark_session(optimized=False)
perform_delta_operations(spark_non_optimized, s3_non_optimized_path, optimized=False)

# COMMAND ----------

# Compare file counts and sizes
def compare_table_stats(optimized_path, non_optimized_path):
    print("\nComparison of Table Statistics:")

    optimized_stats = spark.sql(f"DESCRIBE DETAIL delta.`{optimized_path}`").collect()[0]
    non_optimized_stats = spark.sql(f"DESCRIBE DETAIL delta.`{non_optimized_path}`").collect()[0]

    print(f"Optimized table:")
    print(f"  - Number of files: {optimized_stats['numFiles']}")
    print(f"  - Size in bytes: {optimized_stats['sizeInBytes']}")

    print(f"\nNon-optimized table:")
    print(f"  - Number of files: {non_optimized_stats['numFiles']}")
    print(f"  - Size in bytes: {non_optimized_stats['sizeInBytes']}")

compare_table_stats(s3_optimized_path, s3_non_optimized_path)