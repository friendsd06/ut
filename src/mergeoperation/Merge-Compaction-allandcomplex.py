# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from delta.tables import *
import time
import random
from datetime import datetime, timedelta
import boto3
import json

# COMMAND ----------

# Initialize Spark Session with S3 configurations
def create_spark_session(app_name, optimized=True):
    builder = SparkSession.builder.appName(app_name) \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.delta.logStore.class", "org.apache.spark.sql.delta.storage.S3SingleDriverLogStore") \
        .config("spark.hadoop.fs.s3a.access.key", dbutils.secrets.get(scope="aws", key="access_key")) \
        .config("spark.hadoop.fs.s3a.secret.key", dbutils.secrets.get(scope="aws", key="secret_key"))

    if optimized:
        builder = builder \
            .config("spark.databricks.delta.optimizeWrite.enabled", "true") \
            .config("spark.databricks.delta.autoCompact.enabled", "true") \
            .config("spark.databricks.delta.merge.optimizeInsertOnlyMerge.enabled", "true") \
            .config("spark.databricks.delta.properties.defaults.enableChangeDataFeed", "true") \
            .config("spark.databricks.delta.merge.enablePartitionOptimization", "true") \
            .config("spark.databricks.delta.stats.skipping", "true") \
            .config("spark.sql.files.maxRecordsPerFile", "5000000")
    else:
        builder = builder \
            .config("spark.databricks.delta.optimizeWrite.enabled", "false") \
            .config("spark.databricks.delta.autoCompact.enabled", "false") \
            .config("spark.databricks.delta.merge.optimizeInsertOnlyMerge.enabled", "false") \
            .config("spark.databricks.delta.properties.defaults.enableChangeDataFeed", "false")

    return builder.getOrCreate()

# COMMAND ----------

# S3 bucket and paths
S3_BUCKET = "your-s3-bucket-name"
OPTIMIZED_PATH = f"s3a://{S3_BUCKET}/delta_optimized_orders"
NON_OPTIMIZED_PATH = f"s3a://{S3_BUCKET}/delta_non_optimized_orders"

# Schema for e-commerce orders
order_schema = StructType([
    StructField("order_id", StringType(), False),
    StructField("customer_id", StringType(), False),
    StructField("order_date", TimestampType(), False),
    StructField("product_id", StringType(), False),
    StructField("quantity", IntegerType(), False),
    StructField("price", DoubleType(), False),
    StructField("shipping_address", StringType(), True),
    StructField("status", StringType(), False)
])

# Generate sample order data
def generate_orders(num_orders, start_date, end_date):
    def random_date(start, end):
        return start + timedelta(seconds=random.randint(0, int((end - start).total_seconds())))

    data = []
    for i in range(num_orders):
        order_id = f"ORD-{i+1:08d}"
        customer_id = f"CUST-{random.randint(1, 100000):06d}"
        order_date = random_date(start_date, end_date)
        product_id = f"PROD-{random.randint(1, 1000):04d}"
        quantity = random.randint(1, 10)
        price = round(random.uniform(10, 1000), 2)
        shipping_address = f"Address-{random.randint(1, 1000)}"
        status = random.choice(["Pending", "Shipped", "Delivered", "Cancelled"])
        data.append((order_id, customer_id, order_date, product_id, quantity, price, shipping_address, status))

    return spark.createDataFrame(data, order_schema)

# COMMAND ----------

# Function to analyze S3 storage
def analyze_s3_storage(path):
    s3 = boto3.client('s3')
    bucket_name = path.split('/')[2]
    prefix = '/'.join(path.split('/')[3:])

    response = s3.list_objects_v2(Bucket=bucket_name, Prefix=prefix)

    total_size = 0
    file_count = 0
    parquet_count = 0
    parquet_size = 0

    for obj in response.get('Contents', []):
        file_count += 1
        total_size += obj['Size']
        if obj['Key'].endswith('.parquet'):
            parquet_count += 1
            parquet_size += obj['Size']

    print(f"\nS3 Storage Analysis for {path}:")
    print(f"Total files: {file_count}")
    print(f"Total size: {total_size / 1024 / 1024:.2f} MB")
    print(f"Parquet files: {parquet_count}")
    print(f"Parquet size: {parquet_size / 1024 / 1024:.2f} MB")

    return total_size, parquet_size, file_count, parquet_count

# COMMAND ----------

# Function to analyze Delta table
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

    # Table history
    history = spark.sql(f"DESCRIBE HISTORY delta.`{path}`")
    print("\nRecent Table History:")
    history.show(3, truncate=False)

    # Data distribution
    data_distribution = spark.read.format("delta").load(path).groupBy("status").count().orderBy(desc("count"))
    print("\nData Distribution by Status:")
    data_distribution.show()

    return details, log_files, history, data_distribution

# COMMAND ----------

# Function to perform and analyze merge operation
def perform_merge_operation(spark, base_path, merge_data, optimized):
    delta_table = DeltaTable.forPath(spark, base_path)

    start_time = time.time()

    delta_table.alias("target").merge(
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
        "shipping_address": "source.shipping_address",
        "status": "source.status"
    }).execute()

    execution_time = time.time() - start_time
    print(f"Merge operation ({'Optimized' if optimized else 'Non-optimized'}) took {execution_time:.2f} seconds")

    # Analyze post-merge state
    details, log_files, history, distribution = analyze_delta_table(spark, base_path, "merge")

    # Analyze S3 storage
    total_size, parquet_size, file_count, parquet_count = analyze_s3_storage(base_path)

    return execution_time, details, log_files, history, distribution, total_size, parquet_size, file_count, parquet_count

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
initial_data.write.format("delta").mode("overwrite").partitionBy("status").save(OPTIMIZED_PATH)
analyze_delta_table(spark_optimized, OPTIMIZED_PATH, "initial write (optimized)")
analyze_s3_storage(OPTIMIZED_PATH)

# Perform merge
optimized_results = perform_merge_operation(spark_optimized, OPTIMIZED_PATH, merge_data, True)

# COMMAND ----------

# Scenario 2: Non-Optimized Delta Lake on S3
print("\nScenario 2: Non-Optimized Delta Lake on S3")
spark_non_optimized = create_spark_session("NonOptimizedDeltaS3", optimized=False)

# Write initial data
initial_data.write.format("delta").mode("overwrite").partitionBy("status").save(NON_OPTIMIZED_PATH)
analyze_delta_table(spark_non_optimized, NON_OPTIMIZED_PATH, "initial write (non-optimized)")
analyze_s3_storage(NON_OPTIMIZED_PATH)

# Perform merge
non_optimized_results = perform_merge_operation(spark_non_optimized, NON_OPTIMIZED_PATH, merge_data, False)

# COMMAND ----------

# Comparative Analysis
print("\n--- Comparative Analysis ---")
opt_time, opt_details, opt_logs, opt_history, opt_dist, opt_total_size, opt_parquet_size, opt_file_count, opt_parquet_count = optimized_results
non_opt_time, non_opt_details, non_opt_logs, non_opt_history, non_opt_dist, non_opt_total_size, non_opt_parquet_size, non_opt_file_count, non_opt_parquet_count = non_optimized_results

print(f"Merge Performance Improvement: {(non_opt_time - opt_time) / non_opt_time * 100:.2f}%")
print(f"File Count Reduction: {(non_opt_parquet_count - opt_parquet_count) / non_opt_parquet_count * 100:.2f}%")
print(f"Storage Size Reduction: {(non_opt_total_size - opt_total_size) / non_opt_total_size * 100:.2f}%")

# COMMAND ----------

# Query Performance Analysis
def analyze_query_performance(spark, path, query_type):
    start_time = time.time()

    if query_type == "full_scan":
        spark.read.format("delta").load(path).agg(count("*"), sum("quantity"), avg("price")).collect()
    elif query_type == "filtered":
        spark.read.format("delta").load(path).filter((col("status") == "Delivered") & (col("order_date") > "2023-06-01")).groupBy("product_id").agg(sum("quantity"), avg("price")).collect()

    execution_time = time.time() - start_time
    print(f"{query_type.capitalize()} query on {path.split('/')[-1]} took {execution_time:.2f} seconds")
    return execution_time

print("\n--- Query Performance Analysis ---")
opt_full_scan = analyze_query_performance(spark_optimized, OPTIMIZED_PATH, "full_scan")
non_opt_full_scan = analyze_query_performance(spark_non_optimized, NON_OPTIMIZED_PATH, "full_scan")
opt_filtered = analyze_query_performance(spark_optimized, OPTIMIZED_PATH, "filtered")
non_opt_filtered = analyze_query_performance(spark_non_optimized, NON_OPTIMIZED_PATH, "filtered")

print(f"\nFull Scan Query Improvement: {(non_opt_full_scan - opt_full_scan) / non_opt_full_scan * 100:.2f}%")
print(f"Filtered Query Improvement: {(non_opt_filtered - opt_filtered) / non_opt_filtered * 100:.2f}%")

# COMMAND ----------

# Analyze data skipping efficiency
def analyze_data_skipping(spark, path):
    delta_table = DeltaTable.forPath(spark, path)
    metrics = delta_table.detail().select("numFilesRemoved", "numFilesAdded", "numFilesSkipped", "numFilesAdded", "numFilesRemoved").collect()[0]

    total_files = metrics["numFilesAdded"] + metrics["numFilesRemoved"]
    skipped_percentage = (metrics["numFilesSkipped"] / total_files) * 100 if total_files > 0 else 0

    print(f"\nData Skipping Analysis for {path.split('/')[-1]}:")
    print(f"Files added: {metrics['numFilesAdded']}")
    print(f"Files removed: {metrics['numFilesRemoved']}")
    print(f"Files skipped: {metrics['numFilesSkipped']}")
    print(f"Skipping efficiency: {skipped_percentage:.2f}%")

analyze_data_skipping(spark_optimized, OPTIMIZED_PATH)
analyze_data_skipping(spark_non_optimized, NON_OPTIMIZED_PATH)

# COMMAND ----------

# COMMAND ----------

# Analyze transaction log and file layout (continued)
def analyze_transaction_log_and_layout(path):
    s3 = boto3.client('s3')
    bucket_name = path.split('/')[2]
    prefix = '/'.join(path.split('/')[3:])

    # Analyze transaction log
    log_files = s3.list_objects_v2(Bucket=bucket_name, Prefix=f"{prefix}/_delta_log")
    latest_log = sorted([obj['Key'] for obj in log_files.get('Contents', []) if obj['Key'].endswith('.json')])[-1]
    log_content = s3.get_object(Bucket=bucket_name, Key=latest_log)['Body'].read().decode('utf-8')
    log_json = json.loads(log_content)

    print(f"\nTransaction Log Analysis for {path.split('/')[-1]}:")
    print(f"Total log entries: {log_files['KeyCount']}")
    print(f"Latest commit info:")
    print(json.dumps(log_json.get('commitInfo', {}), indent=2))

    add_actions = [action for action in log_json.get('add', []) if 'path' in action]
    remove_actions = [action for action in log_json.get('remove', []) if 'path' in action]

    print(f"Add actions: {len(add_actions)}")
    print(f"Remove actions: {len(remove_actions)}")

    if add_actions:
        print("\nSample add action:")
        print(json.dumps(add_actions[0], indent=2))

    # Analyze file layout
    parquet_files = s3.list_objects_v2(Bucket=bucket_name, Prefix=prefix)
    partition_layout = {}

    for obj in parquet_files.get('Contents', []):
        if obj['Key'].endswith('.parquet'):
            parts = obj['Key'].split('/')
            partition = '/'.join(parts[:-1])
            if partition not in partition_layout:
                partition_layout[partition] = []
            partition_layout[partition].append(obj['Size'])

    print("\nFile Layout Analysis:")
    for partition, sizes in partition_layout.items():
        print(f"Partition: {partition}")
        print(f"  Files: {len(sizes)}")
        print(f"  Total Size: {sum(sizes) / 1024 / 1024:.2f} MB")
        print(f"  Avg File Size: {sum(sizes) / len(sizes) / 1024 / 1024:.2f} MB")

    return log_json, partition_layout

# Analyze both optimized and non-optimized tables
opt_log, opt_layout = analyze_transaction_log_and_layout(OPTIMIZED_PATH)
non_opt_log, non_opt_layout = analyze_transaction_log_and_layout(NON_OPTIMIZED_PATH)

# COMMAND ----------

# S3 request analysis
def analyze_s3_requests(path, operation):
    s3 = boto3.client('s3')
    bucket_name = path.split('/')[2]
    prefix = '/'.join(path.split('/')[3:])

    # Get S3 request metrics (Note: This requires S3 request metrics to be enabled on the bucket)
    end_time = datetime.now()
    start_time = end_time - timedelta(hours=1)  # Analyze last hour of requests

    response = s3.get_metric_statistics(
        Namespace='AWS/S3',
        MetricName='AllRequests',
        Dimensions=[
            {'Name': 'BucketName', 'Value': bucket_name},
            {'Name': 'FilterId', 'Value': prefix}
        ],
        StartTime=start_time,
        EndTime=end_time,
        Period=3600,
        Statistics=['Sum']
    )

    total_requests = sum([datapoint['Sum'] for datapoint in response['Datapoints']])

    print(f"\nS3 Request Analysis for {operation} on {path.split('/')[-1]}:")
    print(f"Total S3 requests: {total_requests}")

    return total_requests

# Analyze S3 requests for both optimized and non-optimized operations
opt_requests = analyze_s3_requests(OPTIMIZED_PATH, "merge")
non_opt_requests = analyze_s3_requests(NON_OPTIMIZED_PATH, "merge")

print(f"\nS3 Request Reduction: {(non_opt_requests - opt_requests) / non_opt_requests * 100:.2f}%")

# COMMAND ----------

# Compaction analysis
def analyze_compaction(spark, path):
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
    print(f"Pre-compaction size: {pre_compaction_stats['sizeInBytes'] / 1024 / 1024:.2f} MB")
    print(f"Post-compaction size: {post_compaction_stats['sizeInBytes'] / 1024 / 1024:.2f} MB")

    return pre_compaction_stats, post_compaction_stats, compaction_time

# Analyze compaction for both optimized and non-optimized tables
opt_pre, opt_post, opt_time = analyze_compaction(spark_optimized, OPTIMIZED_PATH)
non_opt_pre, non_opt_post, non_opt_time = analyze_compaction(spark_non_optimized, NON_OPTIMIZED_PATH)

print(f"\nCompaction Efficiency Improvement: {(non_opt_time - opt_time) / non_opt_time * 100:.2f}%")

# COMMAND ----------

# Cost analysis (estimated)
def estimate_s3_costs(total_size, request_count):
    # S3 Standard storage cost per GB per month (example rate, adjust as needed)
    storage_cost_per_gb = 0.023
    # S3 PUT/COPY/POST/LIST requests per 1,000 requests (example rate, adjust as needed)
    request_cost_per_1000 = 0.005

    storage_cost = (total_size / 1024 / 1024 / 1024) * storage_cost_per_gb
    request_cost = (request_count / 1000) * request_cost_per_1000

    return storage_cost, request_cost

opt_storage_cost, opt_request_cost = estimate_s3_costs(opt_total_size, opt_requests)
non_opt_storage_cost, non_opt_request_cost = estimate_s3_costs(non_opt_total_size, non_opt_requests)

print("\nEstimated S3 Cost Analysis (per month):")
print(f"Optimized - Storage: ${opt_storage_cost:.2f}, Requests: ${opt_request_cost:.2f}")
print(f"Non-Optimized - Storage: ${non_opt_storage_cost:.2f}, Requests: ${non_opt_request_cost:.2f}")
print(f"Total Cost Savings: ${(non_opt_storage_cost + non_opt_request_cost - opt_storage_cost - opt_request_cost):.2f}")

# COMMAND ----------

# Final summary and recommendations
print("\n--- Final Summary and Recommendations ---")
print("1. Performance:")
print(f"   - Merge operation speedup: {(non_opt_time - opt_time) / non_opt_time * 100:.2f}%")
print(f"   - Query performance improvement: {(non_opt_filtered - opt_filtered) / non_opt_filtered * 100:.2f}%")
print("2. Storage Efficiency:")
print(f"   - File count reduction: {(non_opt_parquet_count - opt_parquet_count) / non_opt_parquet_count * 100:.2f}%")
print(f"   - Storage size reduction: {(non_opt_total_size - opt_total_size) / non_opt_total_size * 100:.2f}%")
print("3. S3 Optimization:")
print(f"   - S3 request reduction: {(non_opt_requests - opt_requests) / non_opt_requests * 100:.2f}%")
print(f"   - Estimated cost savings: ${(non_opt_storage_cost + non_opt_request_cost - opt_storage_cost - opt_request_cost):.2f} per month")
print("4. Data Skipping Efficiency:")
print(f"   - Optimized skipping efficiency: {(opt_details['numFilesSkipped'] / opt_details['numFiles']) * 100:.2f}%")
print(f"   - Non-optimized skipping efficiency: {(non_opt_details['numFilesSkipped'] / non_opt_details['numFiles']) * 100:.2f}%")

print("\nRecommendations:")
print("1. Enable Delta Lake optimizations for improved performance and cost-efficiency.")
print("2. Regularly monitor and adjust partition strategies based on query patterns.")
print("3. Implement automated compaction jobs to maintain optimal file sizes.")
print("4. Use data skipping and Z-ordering for frequently filtered columns.")
print("5. Monitor S3 request patterns and optimize data access to reduce costs.")
print("6. Consider using S3 Intelligent-Tiering for auto-optimizing storage costs based on access patterns.")