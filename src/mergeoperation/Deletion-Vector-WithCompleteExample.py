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

# Initialize Spark Session with advanced configurations
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
            .config("spark.sql.files.maxRecordsPerFile", "1000000") \
            .config("spark.databricks.delta.properties.defaults.enableDeletionVectors", "true") \
            .config("spark.databricks.delta.properties.defaults.columnMapping.mode", "name") \
            .config("spark.databricks.delta.merge.enableDeleteVectorization", "true")
    else:
        builder = builder \
            .config("spark.databricks.delta.optimizeWrite.enabled", "false") \
            .config("spark.databricks.delta.autoCompact.enabled", "false") \
            .config("spark.databricks.delta.merge.optimizeInsertOnlyMerge.enabled", "false") \
            .config("spark.databricks.delta.properties.defaults.enableChangeDataFeed", "false") \
            .config("spark.databricks.delta.stats.skipping", "false") \
            .config("spark.sql.shuffle.partitions", "200") \
            .config("spark.databricks.delta.properties.defaults.enableDeletionVectors", "false")

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

# Generate sample order data (same as before)
def generate_orders(num_orders, start_date, end_date):
# ... (keep the existing implementation)

# COMMAND ----------

# Function to analyze Delta table details (extended)
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

    # Check for deletion vectors
    if 'deletionVectors' in commit_json:
        print("\nDeletion Vector Information:")
        print(f"Number of Deletion Vectors: {len(commit_json['deletionVectors'])}")
        print(f"Sample Deletion Vector: {commit_json['deletionVectors'][0] if commit_json['deletionVectors'] else 'None'}")

    # Table history
    history = spark.sql(f"DESCRIBE HISTORY delta.`{path}`")
    print("\nRecent Table History:")
    history.show(3, truncate=False)

    # Data distribution
    data_distribution = spark.read.format("delta").load(path).groupBy("category", "status").count().orderBy(desc("count"))
    print("\nData Distribution:")
    data_distribution.show(10)

    # Check for column mapping
    column_mapping = spark.sql(f"DESCRIBE DETAIL delta.`{path}`").select("columnMappingMode").collect()[0][0]
    print(f"\nColumn Mapping Mode: {column_mapping}")

    return details, log_files, commit_json, history, data_distribution

# COMMAND ----------

# Function to perform and analyze merge operation (extended)
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

# Function to demonstrate deletion vectors
def demonstrate_deletion_vectors(spark, path):
    delta_table = DeltaTable.forPath(spark, path)

    # Perform a delete operation
    start_time = time.time()
    delete_result = delta_table.delete("status = 'Cancelled'")
    delete_time = time.time() - start_time

    print(f"\nDeletion Operation took {delete_time:.2f} seconds")
    print("Deletion Metrics:")
    delete_result.show()

    # Analyze the table after deletion
    details, log_files, commit_json, history, distribution = analyze_delta_table(spark, path, "deletion")

    # Check for deletion vectors in the commit
    if 'deletionVectors' in commit_json:
        print("\nDeletion Vector Details:")
        for dv in commit_json['deletionVectors']:
            print(f"File Path: {dv['path']}")
            print(f"Deleted Row Count: {dv['deletedRowCount']}")
    else:
        print("\nNo Deletion Vectors found in this commit")

    return delete_time, delete_result, commit_json

# COMMAND ----------

# Function to demonstrate column mapping
def demonstrate_column_mapping(spark, path):
    delta_table = DeltaTable.forPath(spark, path)

    # Rename a column
    delta_table.updateColumnMetadata("product_id", "item_id")

    # Add a new column
    delta_table.updateColumnMetadata("discount", DoubleType())

    # Analyze the table after column mapping changes
    details, log_files, commit_json, history, distribution = analyze_delta_table(spark, path, "column mapping")

    print("\nUpdated Schema:")
    spark.read.format("delta").load(path).printSchema()

    return commit_json

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

# Demonstrate deletion vectors
opt_delete_time, opt_delete_result, opt_delete_commit = demonstrate_deletion_vectors(spark_optimized, OPTIMIZED_PATH)

# Demonstrate column mapping
opt_column_mapping_commit = demonstrate_column_mapping(spark_optimized, OPTIMIZED_PATH)

# COMMAND ----------

# Scenario 2: Non-Optimized Delta Lake on S3
print("\nScenario 2: Non-Optimized Delta Lake on S3")
spark_non_optimized = create_spark_session("NonOptimizedDeltaS3", optimized=False)

# Write initial data
initial_data.write.format("delta").mode("overwrite").partitionBy("category", "status").save(NON_OPTIMIZED_PATH)
analyze_delta_table(spark_non_optimized, NON_OPTIMIZED_PATH, "initial write (non-optimized)")

# Perform merge
non_opt_merge_time, non_opt_details, non_opt_logs, non_opt_commit, non_opt_history, non_opt_distribution, non_opt_metrics = perform_merge_operation(spark_non_optimized, NON_OPTIMIZED_PATH, merge_data, False)

# Demonstrate deletion vectors (without optimization)
non_opt_delete_time, non_opt_delete_result, non_opt_delete_commit = demonstrate_deletion_vectors(spark_non_optimized, NON_OPTIMIZED_PATH)

# Column mapping is not available in non-optimized scenario

# COMMAND ----------

# Comparative Analysis
print("\n--- Comparative Analysis ---")
print(f"Merge Performance Improvement: {(non_opt_merge_time - opt_merge_time) / non_opt_merge_time * 100:.2f}%")
print(f"Delete Performance Improvement: {(non_opt_delete_time - opt_delete_time) / non_opt_delete_time * 100:.2f}%")

print("\nOptimized Scenario Features:")
print(f"Deletion Vectors: {'Yes' if 'deletionVectors' in opt_delete_commit else 'No'}")
print(f"Column Mapping: {'Yes' if opt_column_mapping_commit else 'No'}")

print("\nNon-Optimized Scenario Features:")
print(f"Deletion Vectors: {'Yes' if 'deletionVectors' in non_opt_delete_commit else 'No'}")
print("Column Mapping: No")

# COMMAND ----------

# Final Summary and Recommendations
print("\n--- Final Summary and Recommendations ---")
print("1. Performance:")
print(f"   - Merge operation speedup: {(non_opt_merge_time - opt_merge_time) / non_opt_merge_time * 100:.2f}%")
print(f"   - Delete operation speedup: {(non_opt_delete_time - opt_delete_time) / non_opt_delete_time * 100:.2f}%")
print("2. Advanced Features:")
print("   - Deletion Vectors: Provide efficient handling of deletes, especially for large datasets")
print("   - Column Mapping: Allows schema evolution without full table rewrites")
print("3. S3 Optimization:")
print("   - Optimized writes and auto compaction reduce S3 API calls and improve query performance")
print("   - Deletion vectors minimize data movement on S3 during delete operations")
print("4. Data Management:")
print("   - Change Data Feed enables efficient incremental processing")
print("   - Schema evolution with column mapping reduces maintenance overhead")

print("\nRecommendations:")
print("1. Enable Delta Lake optimizations for improved performance and cost-efficiency on S3")
print("2. Utilize deletion vectors for efficient delete operations, especially for large datasets")
print("3. Leverage column mapping for flexible schema evolution without costly table rewrites")
print("4. Monitor and adjust partition strategies based on query patterns and data distribution")
print("5. Regularly run OPTIMIZE and VACUUM operations to maintain optimal performance")
print("6. Use Change Data Feed for incremental ETL processes and real-time data integration")