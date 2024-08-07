# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from delta.tables import *
import time
import json
from datetime import datetime, timedelta
import random

# COMMAND ----------

# Initialize Spark Session with detailed configurations
def create_spark_session(app_name, optimized=True):
    builder = SparkSession.builder.appName(app_name) \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

    if optimized:
        builder = builder \
            .config("spark.databricks.delta.optimizeWrite.enabled", "true") \
            .config("spark.databricks.delta.autoCompact.enabled", "true") \
            .config("spark.databricks.delta.merge.optimizeInsertOnlyMerge.enabled", "true") \
            .config("spark.databricks.delta.properties.defaults.enableChangeDataFeed", "true") \
            .config("spark.databricks.delta.schema.autoMerge.enabled", "true") \
            .config("spark.databricks.delta.stats.skipping", "true") \
            .config("spark.databricks.optimizer.dynamicPartitionPruning", "true") \
            .config("spark.sql.shuffle.partitions", "200") \
            .config("spark.sql.files.maxRecordsPerFile", "1000000")
    else:
        builder = builder \
            .config("spark.databricks.delta.optimizeWrite.enabled", "false") \
            .config("spark.databricks.delta.autoCompact.enabled", "false") \
            .config("spark.databricks.delta.merge.optimizeInsertOnlyMerge.enabled", "false") \
            .config("spark.databricks.delta.properties.defaults.enableChangeDataFeed", "false") \
            .config("spark.databricks.delta.stats.skipping", "false") \
            .config("spark.sql.shuffle.partitions", "200")

    return builder.getOrCreate()

# COMMAND ----------

# Define paths for Delta tables
OPTIMIZED_PATH = "/mnt/your-s3-mount/delta_optimized_orders"
NON_OPTIMIZED_PATH = "/mnt/your-s3-mount/delta_non_optimized_orders"

# Schema for e-commerce orders
order_schema = StructType([
    StructField("order_id", StringType(), False),
    StructField("customer_id", StringType(), False),
    StructField("order_date", TimestampType(), False),
    StructField("product_id", StringType(), False),
    StructField("quantity", IntegerType(), False),
    StructField("price", DoubleType(), False),
    StructField("category", StringType(), False),
    StructField("status", StringType(), False)
])

# Generate sample order data
def generate_orders(num_orders, start_date, end_date):
    def random_date(start, end):
        return start + timedelta(seconds=random.randint(0, int((end - start).total_seconds())))

    categories = ["Electronics", "Clothing", "Books", "Home & Garden", "Toys"]
    statuses = ["Pending", "Shipped", "Delivered", "Cancelled"]

    data = []
    for i in range(num_orders):
        order_id = f"ORD-{i+1:08d}"
        customer_id = f"CUST-{random.randint(1, 100000):06d}"
        order_date = random_date(start_date, end_date)
        product_id = f"PROD-{random.randint(1, 1000):04d}"
        quantity = random.randint(1, 10)
        price = round(random.uniform(10, 1000), 2)
        category = random.choice(categories)
        status = random.choice(statuses)
        data.append((order_id, customer_id, order_date, product_id, quantity, price, category, status))

    return spark.createDataFrame(data, order_schema)

# COMMAND ----------

# Function to analyze Delta table details
def analyze_delta_table(spark, path, operation=""):
    print(f"\n--- Delta Table Analysis after {operation} ---")

    # Table details
    details = spark.sql(f"DESCRIBE DETAIL delta.`{path}`").collect()[0]
    print(f"Number of files: {details['numFiles']}")
    print(f"Size in bytes: {details['sizeInBytes']}")
    print(f"Number of partitions: {details['numPartitions']}")

    # Delta log analysis
    log_files = dbutils.fs.ls(f"{path}/_delta_log")
    print(f"\nDelta Log files: {len(log_files)}")

    # Analyze latest commit
    latest_commit = max(int(f.name.split('.')[0]) for f in log_files if f.name.endswith('.json'))
    commit_content = dbutils.fs.head(f"{path}/_delta_log/{latest_commit:020d}.json")
    commit_json = json.loads(commit_content)
    print("\nLatest Commit Details:")
    print(f"Timestamp: {commit_json['commitInfo']['timestamp']}")
    print(f"Operation: {commit_json['commitInfo']['operation']}")
    print(f"Operation Parameters: {commit_json['commitInfo'].get('operationParameters', {})}")
    print(f"Files Added: {len(commit_json.get('add', []))}")
    print(f"Files Removed: {len(commit_json.get('remove', []))}")

    # Table history
    history = spark.sql(f"DESCRIBE HISTORY delta.`{path}`")
    print("\nRecent Table History:")
    history.show(3, truncate=False)

    # Data distribution
    data_distribution = spark.read.format("delta").load(path).groupBy("category", "status").count().orderBy(desc("count"))
    print("\nData Distribution:")
    data_distribution.show(10)

    return details, log_files, commit_json, history, data_distribution

# COMMAND ----------

# Function to perform and analyze merge operation
def perform_merge_operation(spark, base_path, merge_data, optimized):
    delta_table = DeltaTable.forPath(spark, base_path)

    start_time = time.time()

    merge_result = delta_table.alias("target").merge(
        merge_data.alias("source"),
        "target.order_id = source.order_id"
    ).whenMatchedUpdate(set = {
        "status": "source.status",
        "quantity": "source.quantity",
        "price": "source.price"
    }).whenNotMatchedInsert(values = {
        "order_id": "source.order_id",
        "customer_id": "source.customer_id",
        "order_date": "source.order_date",
        "product_id": "source.product_id",
        "quantity": "source.quantity",
        "price": "source.price",
        "category": "source.category",
        "status": "source.status"
    })

    # Collect metrics before execution
    metrics_df = merge_result.execute()

    execution_time = time.time() - start_time
    print(f"Merge operation ({'Optimized' if optimized else 'Non-optimized'}) took {execution_time:.2f} seconds")

    # Display merge metrics
    print("\nMerge Metrics:")
    metrics_df.show()

    # Analyze post-merge state
    details, log_files, commit_json, history, distribution = analyze_delta_table(spark, base_path, "merge")

    return execution_time, details, log_files, commit_json, history, distribution, metrics_df

# COMMAND ----------

# Function to analyze query performance with detailed explain
def analyze_query_performance(spark, path, query_type):
    if query_type == "full_scan":
        query = spark.read.format("delta").load(path).groupBy("category").agg(
            count("*").alias("order_count"),
            sum("quantity").alias("total_quantity"),
            avg("price").alias("avg_price")
        )
    elif query_type == "filtered":
        query = spark.read.format("delta").load(path).filter(
            (col("status") == "Delivered") &
            (col("order_date") > "2023-06-01") &
            (col("category") == "Electronics")
        ).groupBy("product_id").agg(
            count("*").alias("order_count"),
            sum("quantity").alias("total_quantity"),
            avg("price").alias("avg_price")
        )

    start_time = time.time()
    result = query.collect()
    execution_time = time.time() - start_time

    print(f"\n{query_type.capitalize()} Query Execution Plan:")
    query.explain(mode="formatted")

    print(f"\n{query_type.capitalize()} query on {path.split('/')[-1]} took {execution_time:.2f} seconds")
    print("Sample Result:")
    spark.createDataFrame(result).show(5, truncate=False)

    return execution_time, query.explain(mode="formatted")

# COMMAND ----------

# Function to analyze data skipping and partition pruning
def analyze_data_skipping(spark, path):
    delta_table = DeltaTable.forPath(spark, path)
    metrics = delta_table.detail().select("numFiles", "numFilesSkipped").collect()[0]

    total_files = metrics["numFiles"]
    skipped_files = metrics["numFilesSkipped"]
    skipped_percentage = (skipped_files / total_files) * 100 if total_files > 0 else 0

    print(f"\nData Skipping Analysis for {path.split('/')[-1]}:")
    print(f"Total files: {total_files}")
    print(f"Files skipped: {skipped_files}")
    print(f"Skipping efficiency: {skipped_percentage:.2f}%")

    # Analyze partition pruning
    pruning_query = spark.read.format("delta").load(path).filter(
        (col("category") == "Electronics") &
        (col("status") == "Delivered")
    )

    print("\nPartition Pruning Analysis:")
    pruning_query.explain(mode="formatted")

    return metrics, pruning_query.explain(mode="formatted")

# COMMAND ----------

# Function to analyze compaction and Z-Order
def analyze_compaction_and_zorder(spark, path):
    delta_table = DeltaTable.forPath(spark, path)

    # Get file stats before compaction
    pre_compaction_stats = delta_table.detail().select("numFiles", "sizeInBytes").collect()[0]

    # Perform compaction
    start_time = time.time()
    delta_table.optimize().executeCompaction()
    compaction_time = time.time() - start_time

    # Get file stats after compaction
    post_compaction_stats = delta_table.detail().select("numFiles", "sizeInBytes").collect()[0]

    print(f"\nCompaction Analysis for {path.split('/')[-1]}:")
    print(f"Compaction time: {compaction_time:.2f} seconds")
    print(f"Pre-compaction files: {pre_compaction_stats['numFiles']}")
    print(f"Post-compaction files: {post_compaction_stats['numFiles']}")
    print(f"File count reduction: {(pre_compaction_stats['numFiles'] - post_compaction_stats['numFiles']) / pre_compaction_stats['numFiles'] * 100:.2f}%")

    # Perform Z-Order
    start_time = time.time()
    delta_table.optimize().executeZorder("category", "order_date")
    zorder_time = time.time() - start_time

    print(f"\nZ-Order Analysis:")
    print(f"Z-Order time: {zorder_time:.2f} seconds")

    # Analyze query after Z-Order
    zorder_query = spark.read.format("delta").load(path).filter(
        (col("category") == "Electronics") &
        (col("order_date") > "2023-06-01")
    )

    print("\nQuery Plan after Z-Order:")
    zorder_query.explain(mode="formatted")

    return pre_compaction_stats, post_compaction_stats, compaction_time, zorder_time, zorder_query.explain(mode="formatted")

# COMMAND ----------

# Generate initial and merge datasets
start_date = datetime(2023, 1, 1)
end_date = datetime(2023, 12, 31)
initial_data = generate_orders(1000000, start_date, end_date)
merge_data = generate_orders(500000, start_date, end_date)

# COMMAND ----------

# Scenario 1: Optimized Delta Lake on S3
print("Scenario 1: Optimized Delta Lake on S3")
spark_optimized = create_spark_session("OptimizedDeltaS3", optimized=True)

# Write initial data
initial_data.write.format("delta").mode("overwrite").partitionBy("category", "status").save(OPTIMIZED_PATH)
analyze_delta_table(spark_optimized, OPTIMIZED_PATH, "initial write (optimized)")

# Perform merge
opt_merge_time, opt_details, opt_logs, opt_commit, opt_history, opt_distribution, opt_metrics = perform_merge_operation(spark_optimized, OPTIMIZED_PATH, merge_data, True)

# Analyze query performance
opt_full_scan_time, opt_full_scan_plan = analyze_query_performance(spark_optimized, OPTIMIZED_PATH, "full_scan")
opt_filtered_time, opt_filtered_plan = analyze_query_performance(spark_optimized, OPTIMIZED_PATH, "filtered")

# Analyze data skipping and partition pruning
opt_skipping_metrics, opt_pruning_plan = analyze_data_skipping(spark_optimized, OPTIMIZED_PATH)

# Analyze compaction and Z-Order
opt_pre_compact, opt_post_compact, opt_compact_time, opt_zorder_time, opt_zorder_plan = analyze_compaction_and_zorder(spark_optimized, OPTIMIZED_PATH)

# COMMAND ----------

# Scenario 2: Non-Optimized Delta Lake on S3
print("\nScenario 2: Non-Optimized Delta Lake on S3")
spark_non_optimized = create_spark_session("NonOptimizedDeltaS3", optimized=False)

# Write initial data
initial_data.write.format("delta").mode("overwrite").partitionBy("category", "status").save(NON_OPTIMIZED_PATH)
analyze_delta_table(spark_non_optimized, NON_OPTIMIZED_PATH, "initial write (non-optimized)")

# Perform merge
non_opt_merge_time, non_opt_details, non_opt_logs, non_opt_commit, non_opt_history, non_opt_distribution, non_opt_metrics = perform_merge_operation(spark_non_optimized, NON_OPTIMIZED_PATH, merge_data, False)

# Analyze query performance
non_opt_full_scan_time, non_opt_full_scan_plan = analyze_query_performance(spark_non_optimized, NON_OPTIMIZED_PATH, "full_scan")
non_opt_filtered_time