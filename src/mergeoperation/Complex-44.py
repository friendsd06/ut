# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from delta.tables import *
import time
from datetime import datetime, timedelta
import random
import json

spark = SparkSession.builder \
    .appName("AdvancedDeltaMergeS3") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.databricks.delta.optimizeWrite.enabled", "true") \
    .config("spark.databricks.delta.autoCompact.enabled", "true") \
    .config("spark.databricks.delta.properties.defaults.enableChangeDataFeed", "true") \
    .config("spark.databricks.delta.schema.autoMerge.enabled", "true") \
    .config("spark.sql.shuffle.partitions", "200") \
    .getOrCreate()

S3_BASE_PATH = "s3a://your-bucket/advanced-delta-merge-examples/"

def time_operation(func):
    def wrapper(*args, **kwargs):
        start_time = time.time()
        result = func(*args, **kwargs)
        end_time = time.time()
        print(f"{func.__name__} took {end_time - start_time:.2f} seconds")
        return result
    return wrapper

# COMMAND ----------

# Use Case 1: Multi-dimensional Slowly Changing Dimension with History Tracking
@time_operation
def multi_dimensional_scd():
    # Define schema for employee data
    employee_schema = StructType([
        StructField("employee_id", StringType(), False),
        StructField("name", StringType(), True),
        StructField("department", StringType(), True),
        StructField("salary", DoubleType(), True),
        StructField("manager_id", StringType(), True),
        StructField("office_location", StringType(), True),
        StructField("effective_date", DateType(), True),
        StructField("end_date", DateType(), True),
        StructField("is_current", BooleanType(), True),
        StructField("change_type", StringType(), True)
    ])

    # Create initial employee data
    initial_employees = spark.createDataFrame([
        ("E001", "John Doe", "IT", 75000.0, "M001", "New York", datetime(2023, 1, 1).date(), None, True, "Initial"),
        ("E002", "Jane Smith", "HR", 65000.0, "M002", "Los Angeles", datetime(2023, 1, 1).date(), None, True, "Initial")
    ], schema=employee_schema)

    initial_employees.write.format("delta").mode("overwrite").save(S3_BASE_PATH + "employees_scd")

    # New employee data with various changes
    new_employees = spark.createDataFrame([
        ("E001", "John Doe", "IT", 80000.0, "M001", "New York", datetime(2023, 6, 1).date(), None, True, "Salary Change"),
        ("E002", "Jane Smith", "Marketing", 70000.0, "M003", "Chicago", datetime(2023, 6, 1).date(), None, True, "Department and Location Change"),
        ("E003", "Alice Johnson", "Finance", 72000.0, "M002", "Boston", datetime(2023, 6, 1).date(), None, True, "New Employee")
    ], schema=employee_schema)

    # Perform multi-dimensional SCD merge
    employee_table = DeltaTable.forPath(spark, S3_BASE_PATH + "employees_scd")

    # Step 1: End-date the current records that will be updated
    employee_table.alias("current").merge(
        new_employees.alias("updates"),
        "current.employee_id = updates.employee_id AND current.is_current = true"
    ).whenMatchedUpdate(set = {
        "end_date": "updates.effective_date",
        "is_current": "false"
    }).execute()

    # Step 2: Insert new records
    employee_table.alias("current").merge(
        new_employees.alias("updates"),
        "current.employee_id = updates.employee_id AND current.is_current = false"
    ).whenNotMatchedInsert(values = {
        "employee_id": "updates.employee_id",
        "name": "updates.name",
        "department": "updates.department",
        "salary": "updates.salary",
        "manager_id": "updates.manager_id",
        "office_location": "updates.office_location",
        "effective_date": "updates.effective_date",
        "end_date": "null",
        "is_current": "true",
        "change_type": "updates.change_type"
    }).execute()

    # Show updated employee data
    updated_employees = spark.read.format("delta").load(S3_BASE_PATH + "employees_scd")
    print("Updated Employees (SCD):")
    updated_employees.orderBy("employee_id", "effective_date").show(truncate=False)

# COMMAND ----------

# Use Case 2: Incremental ETL with Advanced Partitioning and Z-Ordering
@time_operation
def incremental_etl_with_optimizations():
    # Define schema for event data
    event_schema = StructType([
        StructField("event_id", StringType(), False),
        StructField("user_id", StringType(), False),
        StructField("event_type", StringType(), True),
        StructField("timestamp", TimestampType(), False),
        StructField("device", StringType(), True),
        StructField("location", StringType(), True),
        StructField("properties", MapType(StringType(), StringType()), True)
    ])

    # Function to generate sample event data
    def generate_events(num_events, start_time):
        events = []
        event_types = ["pageview", "click", "purchase", "login", "logout"]
        devices = ["mobile", "desktop", "tablet"]
        locations = ["US", "UK", "CA", "DE", "FR"]

        for i in range(num_events):
            event_id = f"E{i+1:06d}"
            user_id = f"U{random.randint(1, 1000):04d}"
            event_type = random.choice(event_types)
            timestamp = start_time + timedelta(minutes=random.randint(0, 60*24))
            device = random.choice(devices)
            location = random.choice(locations)
            properties = {
                "page": f"/page_{random.randint(1, 100)}",
                "value": str(random.uniform(1, 1000))
            }
            events.append((event_id, user_id, event_type, timestamp, device, location, properties))

        return spark.createDataFrame(events, schema=event_schema)

    # Create initial event data
    initial_events = generate_events(10000, datetime(2023, 1, 1))
    initial_events.write.format("delta") \
        .mode("overwrite") \
        .partitionBy("event_type") \
        .save(S3_BASE_PATH + "events")

    # Optimize initial data
    delta_table = DeltaTable.forPath(spark, S3_BASE_PATH + "events")
    delta_table.optimize().executeZOrderBy("user_id", "timestamp")

    # Generate new event data
    new_events = generate_events(5000, datetime(2023, 1, 2))

    # Perform incremental merge with advanced optimizations
    merged_data = delta_table.alias("existing").merge(
        new_events.alias("new"),
        "existing.event_id = new.event_id"
    ).whenMatchedUpdateAll() \
        .whenNotMatchedInsertAll() \
        .execute()

    # Re-optimize after merge
    delta_table.optimize().executeZOrderBy("user_id", "timestamp")

    # Show table details and data sample
    print("Event Table Details:")
    spark.sql(f"DESCRIBE DETAIL delta.`{S3_BASE_PATH}events`").show(truncate=False)

    print("\nEvent Data Sample:")
    spark.read.format("delta").load(S3_BASE_PATH + "events").orderBy(desc("timestamp")).show(5, truncate=False)

    # Demonstrate efficient query
    efficient_query = spark.read.format("delta").load(S3_BASE_PATH + "events") \
        .filter((col("event_type") == "purchase") & (col("timestamp") > "2023-01-01 12:00:00")) \
        .groupBy("user_id") \
        .agg(count("*").alias("purchase_count"), sum(get_json_object(col("properties").getItem("value"), "$")).alias("total_value"))

    print("\nEfficient Query Result:")
    efficient_query.explain(mode="formatted")
    efficient_query.show(5)

# COMMAND ----------

# Use Case 3: Real-time Fraud Detection with Streaming Updates and Complex Joins
@time_operation
def real_time_fraud_detection():
    # Define schemas
    transaction_schema = StructType([
        StructField("transaction_id", StringType(), False),
        StructField("user_id", StringType(), False),
        StructField("amount", DoubleType(), True),
        StructField("timestamp", TimestampType(), False),
        StructField("merchant", StringType(), True),
        StructField("location", StringType(), True)
    ])

    user_profile_schema = StructType([
        StructField("user_id", StringType(), False),
        StructField("risk_score", DoubleType(), True),
        StructField("last_update", TimestampType(), True)
    ])

    # Create initial user profiles
    initial_profiles = spark.createDataFrame([
        ("U001", 0.2, datetime.now() - timedelta(days=30)),
        ("U002", 0.5, datetime.now() - timedelta(days=15)),
        ("U003", 0.1, datetime.now() - timedelta(days=7))
    ], schema=user_profile_schema)

    initial_profiles.write.format("delta").mode("overwrite").save(S3_BASE_PATH + "user_profiles")

    # Function to generate streaming transaction data
    def generate_transactions(num_transactions):
        transactions = []
        for i in range(num_transactions):
            transaction_id = f"T{i+1:06d}"
            user_id = f"U{random.randint(1, 5):03d}"
            amount = random.uniform(10, 1000)
            timestamp = datetime.now() - timedelta(minutes=random.randint(0, 60))
            merchant = f"Merchant-{random.randint(1, 20)}"
            location = random.choice(["US", "UK", "CA", "DE", "FR"])
            transactions.append((transaction_id, user_id, amount, timestamp, merchant, location))
        return spark.createDataFrame(transactions, schema=transaction_schema)

    # Simulate streaming transactions
    for _ in range(5):  # Simulate 5 micro-batches
        new_transactions = generate_transactions(1000)

        # Write new transactions to a Delta table (simulating streaming write)
        new_transactions.write.format("delta").mode("append").save(S3_BASE_PATH + "transactions")

        # Perform fraud detection logic
        transactions = spark.read.format("delta").load(S3_BASE_PATH + "transactions")
        user_profiles = DeltaTable.forPath(spark, S3_BASE_PATH + "user_profiles")

        # Complex join and fraud detection logic
        fraud_detection_result = transactions.alias("t") \
            .join(user_profiles.toDF().alias("u"), "user_id") \
            .withColumn("is_suspicious",
                        when((col("t.amount") > 500) & (col("u.risk_score") > 0.7), True)
                        .when((col("t.amount") > 1000) & (col("u.risk_score") > 0.5), True)
                        .otherwise(False)) \
            .withColumn("fraud_score",
                        when(col("is_suspicious"), col("t.amount") * col("u.risk_score") / 100)
                        .otherwise(0))

        # Update user profiles based on transaction patterns
        user_profiles.alias("profiles").merge(
            fraud_detection_result.groupBy("user_id").agg(
                avg("fraud_score").alias("avg_fraud_score"),
                count("*").alias("transaction_count")
            ).alias("updates"),
            "profiles.user_id = updates.user_id"
        ).whenMatchedUpdate(set = {
            "risk_score": "CASE WHEN updates.avg_fraud_score > 0 THEN least(profiles.risk_score + 0.1, 1.0) ELSE greatest(profiles.risk_score - 0.05, 0.0) END",
            "last_update": "current_timestamp()"
        }).execute()

        # Show fraud detection results
        print("\nFraud Detection Results:")
        fraud_detection_result.where("is_suspicious").show(5)

        # Show updated user profiles
        print("\nUpdated User Profiles:")
        spark.read.format("delta").load(S3_BASE_PATH + "user_profiles").orderBy(desc("risk_score")).show(5)

# COMMAND ----------

# Use Case 4: Advanced Data Quality and Governance with Delta Lake
@time_operation
def data_quality_and_governance():
    # Define schema for financial transactions
    transaction_schema = StructType([
        StructField("transaction_id", StringType(), False),
        StructField("account_id", StringType(), False),
        StructField("transaction_type", StringType(), True),
        StructField("amount", DoubleType(), True),
        StructField("currency", StringType(), True),
        StructField("timestamp", TimestampType(), False),
        StructField("description", StringType(), True),
        StructField("metadata", MapType(StringType(), StringType()), True)
    ])

    # Create initial transaction data
    initial_transactions = spark.createDataFrame([
        ("T001", "A001", "DEPOSIT", 1000.0, "USD", datetime.now(), "Initial deposit", {"source": "BANK_TRANSFER"}),
        ("T002", "A002", "WITHDRAWAL", 500.0, "EUR", datetime.now(), "ATM withdrawal", {"location": "PARIS"})
    ], schema=transaction_schema)

    initial_transactions.write.format("delta").mode("overwrite").save(S3_BASE_PATH + "financial_transactions")

# COMMAND ----------

# Use Case 4: Advanced Data Quality and Governance with Delta Lake (continued)
@time_operation
def data_quality_and_governance():
    # ... (previous code remains the same)

    # Function to generate new transaction data (including some invalid data)
    def generate_new_transactions():
        return spark.createDataFrame([
            ("T003", "A001", "TRANSFER", 750.0, "USD", datetime.now(), "Transfer to savings", {"destination": "A003"}),
            ("T004", "A002", "WITHDRAWAL", -200.0, "EUR", datetime.now(), "Invalid withdrawal", None),
            ("T005", "A003", "UNKNOWN", 300.0, None, datetime.now() + timedelta(days=1), "Future transaction", {"source": "MOBILE_APP"}),
            ("T006", "A004", "DEPOSIT", 1000.0, "GBP", datetime.now(), "Valid deposit", {"source": "CHEQUE"})
        ], schema=transaction_schema)

    new_transactions = generate_new_transactions()

    # Apply data quality rules
    checked_transactions = apply_data_quality_rules(new_transactions)

    # Perform merge operation with data quality checks
    transaction_table = DeltaTable.forPath(spark, S3_BASE_PATH + "financial_transactions")

    merge_result = transaction_table.alias("target").merge(
        checked_transactions.alias("source"),
        "target.transaction_id = source.transaction_id"
    ).whenMatchedUpdateAll() \
        .whenNotMatchedInsert(
        condition = "source.quality_check = 'PASS'",
        values = {
            "transaction_id": "source.transaction_id",
            "account_id": "source.account_id",
            "transaction_type": "source.transaction_type",
            "amount": "source.amount",
            "currency": "source.currency",
            "timestamp": "source.timestamp",
            "description": "source.description",
            "metadata": "source.metadata"
        }
    )

    merge_result.execute()

    # Log failed quality checks
    failed_checks = checked_transactions.filter(col("quality_check") != "PASS")
    failed_checks.write.format("delta").mode("append").save(S3_BASE_PATH + "failed_quality_checks")

    # Show merged data and failed checks
    print("Updated Transactions:")
    spark.read.format("delta").load(S3_BASE_PATH + "financial_transactions").show(truncate=False)

    print("\nFailed Quality Checks:")
    spark.read.format("delta").load(S3_BASE_PATH + "failed_quality_checks").show(truncate=False)

    # Generate data quality report
    quality_report = checked_transactions.groupBy("quality_check").count()
    print("\nData Quality Report:")
    quality_report.show()

# COMMAND ----------

# Use Case 5: Advanced Time Travel and Data Reconstruction
@time_operation
def advanced_time_travel():
    # Define schema for order data
    order_schema = StructType([
        StructField("order_id", StringType(), False),
        StructField("customer_id", StringType(), False),
        StructField("product_id", StringType(), False),
        StructField("quantity", IntegerType(), True),
        StructField("price", DoubleType(), True),
        StructField("order_date", DateType(), False),
        StructField("status", StringType(), True)
    ])

    # Create initial order data
    initial_orders = spark.createDataFrame([
        ("O001", "C001", "P001", 2, 100.0, datetime(2023, 1, 1).date(), "PLACED"),
        ("O002", "C002", "P002", 1, 200.0, datetime(2023, 1, 2).date(), "PLACED")
    ], schema=order_schema)

    initial_orders.write.format("delta").mode("overwrite").save(S3_BASE_PATH + "orders_timeline")

    # Function to update orders
    def update_orders(version):
        orders = spark.read.format("delta").load(S3_BASE_PATH + "orders_timeline")
        delta_table = DeltaTable.forPath(spark, S3_BASE_PATH + "orders_timeline")

        if version == 1:
            # Update O001 status
            delta_table.update(
                condition = "order_id = 'O001'",
                set = {"status": "'SHIPPED'"}
            )
        elif version == 2:
            # Add new order
            new_order = spark.createDataFrame([
                ("O003", "C003", "P003", 3, 150.0, datetime(2023, 1, 3).date(), "PLACED")
            ], schema=order_schema)
            new_order.write.format("delta").mode("append").save(S3_BASE_PATH + "orders_timeline")
        elif version == 3:
            # Update O002 quantity and price
            delta_table.update(
                condition = "order_id = 'O002'",
                set = {"quantity": "2", "price": "180.0"}
            )
        elif version == 4:
            # Delete O001
            delta_table.delete("order_id = 'O001'")

    # Perform updates
    for i in range(1, 5):
        update_orders(i)
        print(f"Version {i} update completed")

    # Time travel queries
    def show_version(version):
        print(f"\nOrders at version {version}:")
        spark.read.format("delta").option("versionAsOf", version).load(S3_BASE_PATH + "orders_timeline").show()

    for i in range(5):
        show_version(i)

    # Data reconstruction to a point in time
    target_date = datetime(2023, 1, 2, 12, 0, 0)
    history = spark.sql(f"DESCRIBE HISTORY delta.`{S3_BASE_PATH}orders_timeline`")
    target_version = history.filter(col("timestamp") <= target_date).select("version").first()[0]

    print(f"\nReconstructed data as of {target_date}:")
    reconstructed_data = spark.read.format("delta").option("versionAsOf", target_version).load(S3_BASE_PATH + "orders_timeline")
    reconstructed_data.show()

# COMMAND ----------

# Use Case 6: Complex Event Processing with Delta Lake and Structured Streaming
@time_operation
def complex_event_processing():
    from pyspark.sql.streaming import DataStreamWriter

    # Define schema for event data
    event_schema = StructType([
        StructField("event_id", StringType(), False),
        StructField("event_type", StringType(), True),
        StructField("device_id", StringType(), True),
        StructField("user_id", StringType(), True),
        StructField("timestamp", TimestampType(), False),
        StructField("value", DoubleType(), True)
    ])

    # Function to generate streaming data
    def generate_streaming_data():
        event_types = ["LOGIN", "CLICK", "PURCHASE", "ERROR"]
        while True:
            event = (
                f"E{random.randint(1, 1000000):07d}",
                random.choice(event_types),
                f"D{random.randint(1, 100):03d}",
                f"U{random.randint(1, 1000):04d}",
                datetime.now(),
                random.uniform(1, 100)
            )
            yield event

    # Create streaming DataFrame
    streaming_df = spark.readStream.format("rate").option("rowsPerSecond", 10).load() \
        .withColumn("event", struct(
        lit("E").alias("event_id"),
        col("value").cast("string").alias("event_type"),
        lit("D").alias("device_id"),
        lit("U").alias("user_id"),
        col("timestamp"),
        rand().alias("value")
    )) \
        .select("event.*")

    # Write streaming data to Delta table
    query = streaming_df.writeStream \
        .format("delta") \
        .outputMode("append") \
        .option("checkpointLocation", S3_BASE_PATH + "checkpoints/events") \
        .start(S3_BASE_PATH + "events_stream")

    # Wait for some data to be written
    time.sleep(30)

    # Perform complex event processing
    events = spark.read.format("delta").load(S3_BASE_PATH + "events_stream")

    # Detect login followed by purchase within 5 minutes
    login_purchase_pattern = events.alias("login") \
        .join(events.alias("purchase"),
              (col("login.user_id") == col("purchase.user_id")) &
              (col("login.event_type") == "LOGIN") &
              (col("purchase.event_type") == "PURCHASE") &
              (col("purchase.timestamp") > col("login.timestamp")) &
              (col("purchase.timestamp") <= col("login.timestamp") + expr("INTERVAL 5 MINUTES"))) \
        .select("login.user_id", "login.timestamp", "purchase.timestamp", "purchase.value")

    print("Login-Purchase Pattern Detection:")
    login_purchase_pattern.show(5)

    # Detect error rate exceeding threshold
    window_duration = "1 minute"
    slide_duration = "30 seconds"
    error_rate_threshold = 0.1

    error_rate = events \
        .withWatermark("timestamp", "5 minutes") \
        .groupBy(
        window("timestamp", window_duration, slide_duration),
        "device_id"
    ) \
        .agg(
        count(when(col("event_type") == "ERROR", 1)).alias("error_count"),
        count("*").alias("total_count")
    ) \
        .withColumn("error_rate", col("error_count") / col("total_count")) \
        .filter(col("error_rate") > error_rate_threshold)

    print("High Error Rate Detection:")
    error_rate.show(5)

    # Stop the streaming query
    query.stop()

# Execute all use cases
multi_dimensional_scd()
incremental_etl_with_optimizations()
real_time_fraud_detection()
data_quality_and_governance()
advanced_time_travel()
complex_event_proce