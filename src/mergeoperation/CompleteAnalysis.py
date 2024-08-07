# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from delta.tables import *
import time
import random
from datetime import datetime, timedelta

# COMMAND ----------

# Helper function to create Spark session with specific configurations
def create_spark_session(merge_optimized=True):
    builder = SparkSession.builder.appName("DeltaMergeOptimizationAnalysis")

    if merge_optimized:
        builder = builder \
            .config("spark.databricks.delta.optimizeWrite.enabled", "true") \
            .config("spark.databricks.delta.autoCompact.enabled", "true") \
            .config("spark.databricks.delta.merge.optimizeInsertOnlyMerge.enabled", "true")
    else:
        builder = builder \
            .config("spark.databricks.delta.optimizeWrite.enabled", "false") \
            .config("spark.databricks.delta.autoCompact.enabled", "false") \
            .config("spark.databricks.delta.merge.optimizeInsertOnlyMerge.enabled", "false")

    return builder.getOrCreate()

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

# Function to generate sample data
def generate_orders(num_orders):
    def random_date(start, end):
        return start + timedelta(days=random.randint(0, (end - start).days))

    start_date = datetime(2023, 1, 1)
    end_date = datetime(2023, 12, 31)

    data = []
    for i in range(num_orders):
        order_id = f"ORD-{i+1:06d}"
        customer_id = f"CUST-{random.randint(1, 1000):04d}"
        order_date = random_date(start_date, end_date)
        product_id = f"PROD-{random.randint(1, 100):03d}"
        quantity = random.randint(1, 10)
        price = round(random.uniform(10, 1000), 2)
        data.append((order_id, customer_id, order_date, product_id, quantity, price))

    return spark.createDataFrame(data, schema)

# COMMAND ----------

# Function to analyze Delta table
def analyze_delta_table(path, operation=""):
    print(f"\n--- Delta Table Analysis after {operation} ---")

    # File statistics
    files = dbutils.fs.ls(path)
    parquet_files = [f for f in files if f.name.endswith('.parquet')]
    print(f"Total files: {len(files)}")
    print(f"Parquet files: {len(parquet_files)}")

    # Size statistics
    total_size = sum(f.size for f in files)
    parquet_size = sum(f.size for f in parquet_files)
    print(f"Total size: {total_size / 1024 / 1024:.2f} MB")
    print(f"Parquet files size: {parquet_size / 1024 / 1024:.2f} MB")

    # Delta Log analysis
    log_files = [f for f in files if f.name.startswith('_delta_log')]
    print(f"\nDelta Log files: {len(log_files)}")

    # Table history
    history = spark.sql(f"DESCRIBE HISTORY delta.`{path}`")
    print("\nTable History:")
    history.show(truncate=False)

    # Detailed statistics
    stats = spark.sql(f"DESCRIBE DETAIL delta.`{path}`").collect()[0]
    print("\nDetailed Statistics:")
    for key, value in stats.asDict().items():
        print(f"{key}: {value}")

# COMMAND ----------

# Function to perform merge operation and measure performance
def perform_merge_operation(spark, base_path, merge_data, optimized):
    delta_table = DeltaTable.forPath(spark, base_path)

    start_time = time.time()

    delta_table.alias("target").merge(
        merge_data.alias("source"),
        "target.order_id = source.order_id"
    ).whenMatchedUpdate(set = {
        "quantity": "source.quantity",
        "price": "source.price"
    }).whenNotMatchedInsert(values = {
        "order_id": "source.order_id",
        "customer_id": "source.customer_id",
        "order_date": "source.order_date",
        "product_id": "source.product_id",
        "quantity": "source.quantity",
        "price": "source.price"
    }).execute()

    execution_time = time.time() - start_time
    print(f"Merge operation ({'Optimized' if optimized else 'Non-optimized'}) took {execution_time:.2f} seconds")

    return execution_time

# COMMAND ----------

# Paths for optimized and non-optimized Delta tables
optimized_path = "/mnt/your-s3-mount/delta_merge_optimized"
non_optimized_path = "/mnt/your-s3-mount/delta_merge_non_optimized"

# Generate initial dataset
initial_data = generate_orders(100000)

# Generate merge dataset (50% updates, 50% inserts)
merge_data = generate_orders(50000)
merge_data = merge_data.union(initial_data.sample(0.25).withColumn("quantity", col("quantity") + 1))

# COMMAND ----------

# Scenario 1: Optimized Merge
print("Scenario 1: Optimized Merge")
spark_optimized = create_spark_session(merge_optimized=True)

# Write initial data
initial_data.write.format("delta").mode("overwrite").save(optimized_path)
analyze_delta_table(optimized_path, "initial write (optimized)")

# Perform merge
optimized_time = perform_merge_operation(spark_optimized, optimized_path, merge_data, True)
analyze_delta_table(optimized_path, "merge (optimized)")

# COMMAND ----------

# Scenario 2: Non-Optimized Merge
print("\nScenario 2: Non-Optimized Merge")
spark_non_optimized = create_spark_session(merge_optimized=False)

# Write initial data
initial_data.write.format("delta").mode("overwrite").save(non_optimized_path)
analyze_delta_table(non_optimized_path, "initial write (non-optimized)")

# Perform merge
non_optimized_time = perform_merge_operation(spark_non_optimized, non_optimized_path, merge_data, False)
analyze_delta_table(non_optimized_path, "merge (non-optimized)")

# COMMAND ----------

# Compare performance and statistics
print("\n--- Performance Comparison ---")
print(f"Optimized merge time: {optimized_time:.2f} seconds")
print(f"Non-optimized merge time: {non_optimized_time:.2f} seconds")
print(f"Performance improvement: {(non_optimized_time - optimized_time) / non_optimized_time * 100:.2f}%")

# Compare file counts
optimized_files = len([f for f in dbutils.fs.ls(optimized_path) if f.name.endswith('.parquet')])
non_optimized_files = len([f for f in dbutils.fs.ls(non_optimized_path) if f.name.endswith('.parquet')])
print(f"\nOptimized merge file count: {optimized_files}")
print(f"Non-optimized merge file count: {non_optimized_files}")
print(f"File count reduction: {(non_optimized_files - optimized_files) / non_optimized_files * 100:.2f}%")

# Compare sizes
optimized_size = sum(f.size for f in dbutils.fs.ls(optimized_path) if f.name.endswith('.parquet'))
non_optimized_size = sum(f.size for f in dbutils.fs.ls(non_optimized_path) if f.name.endswith('.parquet'))
print(f"\nOptimized merge total size: {optimized_size / 1024 / 1024:.2f} MB")
print(f"Non-optimized merge total size: {non_optimized_size / 1024 / 1024:.2f} MB")
print(f"Size reduction: {(non_optimized_size - optimized_size) / non_optimized_size * 100:.2f}%")

# COMMAND ----------

# Analyze query performance
def analyze_query_performance(path, query_type):
    start_time = time.time()

    if query_type == "full_scan":
        spark.read.format("delta").load(path).agg(sum("quantity"), avg("price")).collect()
    elif query_type == "filtered":
        spark.read.format("delta").load(path).filter(col("order_date") > "2023-06-01").groupBy("product_id").agg(sum("quantity")).collect()

    execution_time = time.time() - start_time
    print(f"{query_type.capitalize()} query on {path.split('/')[-1]} took {execution_time:.2f} seconds")
    return execution_time

print("\n--- Query Performance Analysis ---")
optimized_full_scan = analyze_query_performance(optimized_path, "full_scan")
non_optimized_full_scan = analyze_query_performance(non_optimized_path, "full_scan")
optimized_filtered = analyze_query_performance(optimized_path, "filtered")
non_optimized_filtered = analyze_query_performance(non_optimized_path, "filtered")

print(f"\nFull scan query improvement: {(non_optimized_full_scan - optimized_full_scan) / non_optimized_full_scan * 100:.2f}%")
print(f"Filtered query improvement: {(non_optimized_filtered - optimized_filtered) / non_optimized_filtered * 100:.2f}%")

# COMMAND ----------

# Analyze data skipping efficiency
def analyze_data_skipping(path):
    delta_table = DeltaTable.forPath(spark, path)
    metrics = delta_table.detail().select("numFilesRemoved", "numFilesAdded", "numFilesSkipped", "numFilesAdded", "numFilesRemoved").collect()[0]

    total_files = metrics["numFilesAdded"] + metrics["numFilesRemoved"]
    skipped_percentage = (metrics["numFilesSkipped"] / total_files) * 100 if total_files > 0 else 0

    print(f"\nData Skipping Analysis for {path.split('/')[-1]}:")
    print(f"Files added: {metrics['numFilesAdded']}")
    print(f"Files removed: {metrics['numFilesRemoved']}")
    print(f"Files skipped: {metrics['numFilesSkipped']}")
    print(f"Skipping efficiency: {skipped_percentage:.2f}%")

analyze_data_skipping(optimized_path)
analyze_data_skipping(non_optimized_path)

# COMMAND ----------

# Analyze transaction log
def analyze_transaction_log(path):
    log_files = [f for f in dbutils.fs.ls(f"{path}/_delta_log") if f.name.endswith('.json')]
    latest_log = sorted(log_files, key=lambda f: f.name, reverse=True)[0]

    log_content = dbutils.fs.head(latest_log.path)
    log_json = json.loads(log_content)

    print(f"\nTransaction Log Analysis for {path.split('/')[-1]}:")
    print(f"Total log entries: {len(log_files)}")
    print(f"Latest commit info:")
    print(json.dumps(log_json.get('commitInfo', {}), indent=2))

    add_actions = [action for action in log_json.get('add', []) if 'path' in action]
    remove_actions = [action for action in log_json.get('remove', []) if 'path' in action]

    print(f"Add actions: {len(add_actions)}")
    print(f"Remove actions: {len(remove_actions)}")

    if add_actions:
        print("\nSample add action:")
        print(json.dumps(add_actions[0], indent=2))

analyze_transaction_log(optimized_path)
analyze_transaction_log(non_optimized_path)