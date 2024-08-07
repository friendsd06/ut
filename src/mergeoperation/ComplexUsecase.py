# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from delta.tables import *
import time
from datetime import datetime, timedelta
import random

# Initialize Spark Session with advanced configurations
spark = SparkSession.builder \
    .appName("AdvancedDeltaMergeS3") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.databricks.delta.optimizeWrite.enabled", "true") \
    .config("spark.databricks.delta.autoCompact.enabled", "true") \
    .config("spark.databricks.delta.properties.defaults.enableChangeDataFeed", "true") \
    .config("spark.databricks.delta.merge.optimizeInsertOnlyMerge.enabled", "true") \
    .config("spark.databricks.delta.properties.defaults.columnMapping.mode", "name") \
    .config("spark.databricks.delta.schema.autoMerge.enabled", "true") \
    .getOrCreate()

# S3 base path
S3_BASE_PATH = "s3a://your-bucket/delta-merge-examples/"

# Helper function to time operations
def time_operation(func):
    def wrapper(*args, **kwargs):
        start_time = time.time()
        result = func(*args, **kwargs)
        end_time = time.time()
        print(f"{func.__name__} took {end_time - start_time:.2f} seconds")
        return result
    return wrapper

# COMMAND ----------

# Use Case 1: Real-time Customer 360 Update
@time_operation
def customer_360_update():
    # Define schemas
    customer_schema = StructType([
        StructField("customer_id", StringType(), False),
        StructField("name", StringType(), True),
        StructField("email", StringType(), True),
        StructField("last_update", TimestampType(), True)
    ])

    transaction_schema = StructType([
        StructField("transaction_id", StringType(), False),
        StructField("customer_id", StringType(), False),
        StructField("amount", DoubleType(), True),
        StructField("transaction_date", TimestampType(), True)
    ])

    # Create initial customer data
    customers = spark.createDataFrame([
        ("C001", "John Doe", "john@example.com", datetime.now() - timedelta(days=30)),
        ("C002", "Jane Smith", "jane@example.com", datetime.now() - timedelta(days=15))
    ], schema=customer_schema)

    customers.write.format("delta").mode("overwrite").save(S3_BASE_PATH + "customers")

    # Create new transaction data
    new_transactions = spark.createDataFrame([
        ("T001", "C001", 100.0, datetime.now() - timedelta(hours=1)),
        ("T002", "C002", 200.0, datetime.now()),
        ("T003", "C003", 150.0, datetime.now())  # New customer
    ], schema=transaction_schema)

    # Perform merge operation
    customer_table = DeltaTable.forPath(spark, S3_BASE_PATH + "customers")

    customer_table.alias("customers").merge(
        new_transactions.alias("transactions"),
        "customers.customer_id = transactions.customer_id"
    ).whenMatchedUpdate(set = {
        "last_update": "transactions.transaction_date"
    }).whenNotMatchedInsert(values = {
        "customer_id": "transactions.customer_id",
        "name": "concat('New Customer ', transactions.customer_id)",
        "email": "concat(lower(transactions.customer_id), '@example.com')",
        "last_update": "transactions.transaction_date"
    }).execute()

    # Show updated customer data
    updated_customers = spark.read.format("delta").load(S3_BASE_PATH + "customers")
    updated_customers.show()

# COMMAND ----------

# Use Case 2: Slowly Changing Dimension (SCD) Type 2 for Product Catalog
@time_operation
def product_catalog_scd2():
    # Define schema
    product_schema = StructType([
        StructField("product_id", StringType(), False),
        StructField("name", StringType(), True),
        StructField("category", StringType(), True),
        StructField("price", DoubleType(), True),
        StructField("effective_date", DateType(), True),
        StructField("end_date", DateType(), True),
        StructField("is_current", BooleanType(), True)
    ])

    # Create initial product data
    products = spark.createDataFrame([
        ("P001", "Laptop", "Electronics", 999.99, datetime.now().date(), None, True),
        ("P002", "Smartphone", "Electronics", 599.99, datetime.now().date(), None, True)
    ], schema=product_schema)

    products.write.format("delta").mode("overwrite").save(S3_BASE_PATH + "products")

    # Create updated product data
    updated_products = spark.createDataFrame([
        ("P001", "Laptop Pro", "Electronics", 1299.99, datetime.now().date(), None, True),
        ("P003", "Tablet", "Electronics", 399.99, datetime.now().date(), None, True)
    ], schema=product_schema)

    # Perform SCD Type 2 merge
    product_table = DeltaTable.forPath(spark, S3_BASE_PATH + "products")

    product_table.alias("products").merge(
        updated_products.alias("updates"),
        "products.product_id = updates.product_id AND products.is_current = true"
    ).whenMatchedUpdate(set = {
        "end_date": "current_date()",
        "is_current": "false"
    }).whenNotMatchedInsert(values = {
        "product_id": "updates.product_id",
        "name": "updates.name",
        "category": "updates.category",
        "price": "updates.price",
        "effective_date": "updates.effective_date",
        "end_date": "null",
        "is_current": "true"
    }).execute()

    # Insert new records for updated products
    product_table.alias("products").merge(
        updated_products.alias("updates"),
        "products.product_id = updates.product_id AND products.is_current = false"
    ).whenNotMatchedInsert(values = {
        "product_id": "updates.product_id",
        "name": "updates.name",
        "category": "updates.category",
        "price": "updates.price",
        "effective_date": "current_date()",
        "end_date": "null",
        "is_current": "true"
    }).execute()

    # Show updated product data
    updated_products = spark.read.format("delta").load(S3_BASE_PATH + "products")
    updated_products.orderBy("product_id", "effective_date").show()

# COMMAND ----------

# Use Case 3: Streaming IoT Data Deduplication and Aggregation
@time_operation
def iot_data_deduplication():
    # Define schema for IoT data
    iot_schema = StructType([
        StructField("device_id", StringType(), False),
        StructField("timestamp", TimestampType(), False),
        StructField("temperature", DoubleType(), True),
        StructField("humidity", DoubleType(), True)
    ])

    # Create initial aggregated data
    initial_data = spark.createDataFrame([
        ("D001", datetime.now() - timedelta(hours=1), 25.5, 60.0),
        ("D002", datetime.now() - timedelta(hours=1), 26.0, 58.5)
    ], schema=iot_schema)

    initial_data.write.format("delta").mode("overwrite").save(S3_BASE_PATH + "iot_aggregated")

    # Simulate streaming data (in micro-batches)
    def generate_iot_data():
        return spark.createDataFrame([
            ("D001", datetime.now(), 26.0, 61.0),
            ("D001", datetime.now(), 26.0, 61.0),  # Duplicate
            ("D002", datetime.now(), 25.5, 59.0),
            ("D003", datetime.now(), 24.5, 62.5)
        ], schema=iot_schema)

    # Perform merge operation with deduplication and aggregation
    for _ in range(3):  # Simulate 3 micro-batches
        new_data = generate_iot_data()

        iot_table = DeltaTable.forPath(spark, S3_BASE_PATH + "iot_aggregated")

        # Deduplicate new data
        deduplicated_data = new_data.dropDuplicates(["device_id", "timestamp"])

        iot_table.alias("agg").merge(
            deduplicated_data.alias("new"),
            "agg.device_id = new.device_id"
        ).whenMatchedUpdate(set = {
            "timestamp": "new.timestamp",
            "temperature": "(agg.temperature + new.temperature) / 2",  # Average
            "humidity": "(agg.humidity + new.humidity) / 2"  # Average
        }).whenNotMatchedInsert(values = {
            "device_id": "new.device_id",
            "timestamp": "new.timestamp",
            "temperature": "new.temperature",
            "humidity": "new.humidity"
        }).execute()

    # Show final aggregated data
    final_data = spark.read.format("delta").load(S3_BASE_PATH + "iot_aggregated")
    final_data.orderBy("device_id").show()

# COMMAND ----------

# Use Case 4: Multi-Table Transaction with Conditional Updates
@time_operation
def multi_table_transaction():
    # Define schemas
    order_schema = StructType([
        StructField("order_id", StringType(), False),
        StructField("customer_id", StringType(), False),
        StructField("total_amount", DoubleType(), True),
        StructField("status", StringType(), True)
    ])

    inventory_schema = StructType([
        StructField("product_id", StringType(), False),
        StructField("quantity", IntegerType(), True)
    ])

    # Create initial data
    orders = spark.createDataFrame([
        ("O001", "C001", 150.0, "Pending"),
        ("O002", "C002", 200.0, "Shipped")
    ], schema=order_schema)

    inventory = spark.createDataFrame([
        ("P001", 100),
        ("P002", 50)
    ], schema=inventory_schema)

    orders.write.format("delta").mode("overwrite").save(S3_BASE_PATH + "orders")
    inventory.write.format("delta").mode("overwrite").save(S3_BASE_PATH + "inventory")

    # New order data
    new_orders = spark.createDataFrame([
        ("O003", "C001", 300.0, "Pending"),
        ("O001", "C001", 150.0, "Shipped")  # Update existing order
    ], schema=order_schema)

    # Perform multi-table transaction
    order_table = DeltaTable.forPath(spark, S3_BASE_PATH + "orders")
    inventory_table = DeltaTable.forPath(spark, S3_BASE_PATH + "inventory")

    # Update orders
    order_table.alias("orders").merge(
        new_orders.alias("new"),
        "orders.order_id = new.order_id"
    ).whenMatchedUpdate(set = {
        "status": "new.status"
    }).whenNotMatchedInsert(values = {
        "order_id": "new.order_id",
        "customer_id": "new.customer_id",
        "total_amount": "new.total_amount",
        "status": "new.status"
    }).execute()

    # Update inventory (simulating a decrease for new orders)
    inventory_update = new_orders.selectExpr("'P001' as product_id", "1 as quantity")

    inventory_table.alias("inv").merge(
        inventory_update.alias("update"),
        "inv.product_id = update.product_id"
    ).whenMatchedUpdate(set = {
        "quantity": "inv.quantity - update.quantity"
    }).execute()

    # Show updated data
    updated_orders = spark.read.format("delta").load(S3_BASE_PATH + "orders")
    updated_inventory = spark.read.format("delta").load(S3_BASE_PATH + "inventory")

    print("Updated Orders:")
    updated_orders.show()
    print("Updated Inventory:")
    updated_inventory.show()

# COMMAND ----------

# COMMAND ----------

# Use Case 5: Data Quality Enforcement during Merge (continued)
@time_operation
def data_quality_merge():
    # ... (previous code remains the same)

    merged_data = employee_table.alias("emp").merge(
        new_employees.alias("new"),
        "emp.employee_id = new.employee_id"
    ).whenMatchedUpdate(set = {
        "department": "new.department",
        "salary": "CASE WHEN new.salary BETWEEN {} AND {} THEN new.salary ELSE emp.salary END".format(min_salary, max_salary)
    }).whenNotMatchedInsert(values = {
        "employee_id": "new.employee_id",
        "name": "new.name",
        "department": "CASE WHEN new.department IN {} THEN new.department ELSE 'Unknown' END".format(valid_departments),
        "salary": "CASE WHEN new.salary BETWEEN {} AND {} THEN new.salary ELSE {} END".format(min_salary, max_salary, min_salary),
        "hire_date": "CASE WHEN new.hire_date <= current_date() THEN new.hire_date ELSE current_date() END"
    }).execute()

    # Show updated employee data
    updated_employees = spark.read.format("delta").load(S3_BASE_PATH + "employees")
    print("Updated Employees:")
    updated_employees.show()

    # Show rejected records
    rejected_records = new_employees.exceptAll(updated_employees)
    print("Rejected Records:")
    rejected_records.show()

# COMMAND ----------

# Use Case 6: Incremental Data Processing with Change Data Feed
@time_operation
def incremental_processing_with_cdf():
    # Define schema
    sales_schema = StructType([
        StructField("sale_id", StringType(), False),
        StructField("product_id", StringType(), False),
        StructField("sale_date", DateType(), False),
        StructField("quantity", IntegerType(), True),
        StructField("total_amount", DoubleType(), True)
    ])

    # Create initial sales data
    initial_sales = spark.createDataFrame([
        ("S001", "P001", datetime(2023, 1, 1).date(), 5, 500.0),
        ("S002", "P002", datetime(2023, 1, 2).date(), 3, 300.0)
    ], schema=sales_schema)

    initial_sales.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(S3_BASE_PATH + "sales")

    # Enable Change Data Feed
    spark.sql(f"ALTER TABLE delta.`{S3_BASE_PATH}sales` SET TBLPROPERTIES (delta.enableChangeDataFeed = true)")

    # Perform some updates and inserts
    new_sales = spark.createDataFrame([
        ("S003", "P003", datetime(2023, 1, 3).date(), 2, 200.0),
        ("S001", "P001", datetime(2023, 1, 1).date(), 6, 600.0)  # Update
    ], schema=sales_schema)

    sales_table = DeltaTable.forPath(spark, S3_BASE_PATH + "sales")
    sales_table.alias("sales").merge(
        new_sales.alias("new"),
        "sales.sale_id = new.sale_id"
    ).whenMatchedUpdate(set = {
        "quantity": "new.quantity",
        "total_amount": "new.total_amount"
    }).whenNotMatchedInsert(values = {
        "sale_id": "new.sale_id",
        "product_id": "new.product_id",
        "sale_date": "new.sale_date",
        "quantity": "new.quantity",
        "total_amount": "new.total_amount"
    }).execute()

    # Read the Change Data Feed
    cdf = spark.read.format("delta") \
        .option("readChangeFeed", "true") \
        .option("startingVersion", 0) \
        .load(S3_BASE_PATH + "sales")

    print("Change Data Feed:")
    cdf.show()

    # Process the changes (e.g., update a derived table)
    derived_table = cdf.groupBy("product_id") \
        .agg(sum("quantity").alias("total_quantity"),
             sum("total_amount").alias("total_revenue"))

    print("Derived Table (Product Summary):")
    derived_table.show()

# COMMAND ----------

# Use Case 7: Time Travel and Rollback
@time_operation
def time_travel_and_rollback():
    # Define schema
    config_schema = StructType([
        StructField("config_key", StringType(), False),
        StructField("config_value", StringType(), True),
        StructField("last_updated", TimestampType(), True)
    ])

    # Create initial configuration data
    initial_config = spark.createDataFrame([
        ("max_connections", "100", datetime.now()),
        ("timeout", "30", datetime.now())
    ], schema=config_schema)

    initial_config.write.format("delta").mode("overwrite").save(S3_BASE_PATH + "config")

    # Make some changes
    config_table = DeltaTable.forPath(spark, S3_BASE_PATH + "config")

    for i in range(3):
        config_table.update(
            condition = f"config_key = 'max_connections'",
            set = {"config_value": f"{150 + i * 50}", "last_updated": "current_timestamp()"}
        )

    # Show current state
    current_config = spark.read.format("delta").load(S3_BASE_PATH + "config")
    print("Current Configuration:")
    current_config.show()

    # Time travel query
    version_1_config = spark.read.format("delta").option("versionAsOf", 1).load(S3_BASE_PATH + "config")
    print("Configuration at Version 1:")
    version_1_config.show()

    # Rollback to version 1
    config_table.restoreToVersion(1)

    # Verify rollback
    rolled_back_config = spark.read.format("delta").load(S3_BASE_PATH + "config")
    print("Configuration after Rollback:")
    rolled_back_config.show()

# COMMAND ----------

# Use Case 8: Schema Evolution during Merge
@time_operation
def schema_evolution_merge():
    # Define initial schema
    initial_schema = StructType([
        StructField("user_id", StringType(), False),
        StructField("username", StringType(), True),
        StructField("email", StringType(), True)
    ])

    # Create initial user data
    initial_users = spark.createDataFrame([
        ("U001", "john_doe", "john@example.com"),
        ("U002", "jane_smith", "jane@example.com")
    ], schema=initial_schema)

    initial_users.write.format("delta").mode("overwrite").save(S3_BASE_PATH + "users")

    # New data with additional columns
    new_schema = StructType([
        StructField("user_id", StringType(), False),
        StructField("username", StringType(), True),
        StructField("email", StringType(), True),
        StructField("age", IntegerType(), True),
        StructField("country", StringType(), True)
    ])

    new_users = spark.createDataFrame([
        ("U001", "john_doe", "john@example.com", 30, "USA"),
        ("U003", "alice_johnson", "alice@example.com", 25, "Canada")
    ], schema=new_schema)

    # Perform merge with schema evolution
    user_table = DeltaTable.forPath(spark, S3_BASE_PATH + "users")

    user_table.alias("users").merge(
        new_users.alias("new"),
        "users.user_id = new.user_id"
    ).whenMatchedUpdate(set = {
        "username": "new.username",
        "email": "new.email",
        "age": "new.age",
        "country": "new.country"
    }).whenNotMatchedInsert(values = {
        "user_id": "new.user_id",
        "username": "new.username",
        "email": "new.email",
        "age": "new.age",
        "country": "new.country"
    }).execute()

    # Show updated user data
    updated_users = spark.read.format("delta").load(S3_BASE_PATH + "users")
    print("Updated Users with New Schema:")
    updated_users.printSchema()
    updated_users.show()

# COMMAND ----------

# Use Case 9: Conditional Merge with Complex Logic
@time_operation
def conditional_merge_complex_logic():
    # Define schema
    product_schema = StructType([
        StructField("product_id", StringType(), False),
        StructField("name", StringType(), True),
        StructField("category", StringType(), True),
        StructField("price", DoubleType(), True),
        StructField("stock", IntegerType(), True),
        StructField("last_updated", TimestampType(), True)
    ])

    # Create initial product data
    initial_products = spark.createDataFrame([
        ("P001", "Laptop", "Electronics", 999.99, 50, datetime.now() - timedelta(days=30)),
        ("P002", "Smartphone", "Electronics", 599.99, 100, datetime.now() - timedelta(days=15))
    ], schema=product_schema)

    initial_products.write.format("delta").mode("overwrite").save(S3_BASE_PATH + "products")

    # New product data
    new_products = spark.createDataFrame([
        ("P001", "Laptop Pro", "Electronics", 1299.99, 30, datetime.now()),
        ("P002", "Smartphone", "Electronics", 549.99, 150, datetime.now()),
        ("P003", "Tablet", "Electronics", 399.99, 75, datetime.now())
    ], schema=product_schema)

    # Perform conditional merge with complex logic
    product_table = DeltaTable.forPath(spark, S3_BASE_PATH + "products")

    product_table.alias("prod").merge(
        new_products.alias("new"),
        "prod.product_id = new.product_id"
    ).whenMatchedUpdate(condition = """
        (prod.price != new.price AND new.price > prod.price * 1.1) OR
        (prod.stock < 10 AND new.stock > prod.stock) OR
        (datediff(current_date(), prod.last_updated) > 60)
    """, set = {
        "name": "new.name",
        "price": "new.price",
        "stock": "new.stock",
        "last_updated": "new.last_updated"
    }).whenNotMatchedInsert(values = {
        "product_id": "new.product_id",
        "name": "new.name",
        "category": "new.category",
        "price": "new.price",
        "stock": "new.stock",
        "last_updated": "new.last_updated"
    }).execute()

    # Show updated product data
    updated_products = spark.read.format("delta").load(S3_BASE_PATH + "products")
    print("Updated Products:")
    updated_products.orderBy("product_id").show()

# COMMAND ----------

# Use Case 10: Merge with Windowing and Aggregations
@time_operation
def merge_with_windowing():
    # Define schema
    sales_schema = StructType([
        StructField("sale_id", StringType(), False),
        StructField("product_id", StringType(), False),
        StructField("sale_date", DateType(), False),
        StructField("quantity", IntegerType(), True),
        StructField("total_amount", DoubleType(), True),
        StructField("region", StringType(), True)
    ])

    # Create initial sales data
    initial_sales = spark.createDataFrame([
        ("S001", "P001", datetime(2023, 1, 1).date(), 5, 500.0, "North"),
        ("S002", "P002", datetime(2023, 1, 2).date(), 3, 300.0, "South"),
        ("S003", "P001", datetime(2023, 1, 3).date(), 2, 200.0, "East"),
        ("S004", "P003", datetime(2023, 1, 4).date(), 4, 400.0, "West")
    ], schema=sales_schema)

    initial_sales.write.format("delta").mode("overwrite").save(S3_BASE_PATH + "sales_summary")

    # New sales data
    new_sales = spark.createDataFrame([
        ("S005", "P002", datetime(2023, 1, 5).date(), 6, 600.0, "North"),
        ("S006", "P001", datetime(2023, 1, 6).date(), 3, 300.0, "South"),
        ("S007", "P003", datetime(2023, 1, 7).date(), 5, 500.0, "East")
    ], schema=sales_schema)

    # Prepare data with windowing and aggregations
    window_spec = Window.partitionBy("product_id").orderBy("sale_date")

    merged_data = new_sales.withColumn("cumulative_quantity", sum("quantity").over(window_spec)) \
        .withColumn("avg_daily_sales", avg("total_amount").over(window_spec.rowsBetween(-6, 0))) \
        .withColumn("sales_rank", dense_rank().over(Window.partitionBy("region").orderBy(desc("total_amount"))))

    # Perform merge with windowing results
    sales_table = DeltaTable.forPath(spark, S3_BASE_PATH + "sales_summary")

    sales_table.alias("sales").merge(
        merged_data.alias("new"),
        "sales.sale_id = new.sale_id"
    ).whenMatchedUpdate(set = {
        "quantity": "new.quantity",
        "total_amount": "new.total_amount"
    }).whenNotMatchedInsert(values = {
        "sale_id": "new.sale_id",
        "product_id": "new.product_id",
        "sale_date": "new.sale_date",
        "quantity": "new.quantity",
        "total_amount": "new.total_amount",
        "region": "new.region"
    }).execute()

    # Update summary statistics
    sales_table.update(
        set = {
            "quantity": "new.cumulative_quantity",
            "total_amount": "new.avg_daily_sales"
        },
        condition = "sales.product_id = new.product_id AND new.sales_rank = 1"
    )

    # Show updated sales summary
    updated_sales = spark.read.format("delta").load(S3_BASE_PATH + "sales_summary")
    print("Updated Sales Summary:")
    updated_sales.orderBy("sale_date").show()

# COMMAND ----------

# Execute all use cases
customer_360_update()
product_catalog_scd2()
iot_data_deduplication()
multi_table_transaction()
data_quality_merge()
incremental_processing_with_cdf()
time_travel_and_rollback()
schema_evolution_merge()
conditional_merge_complex_logic()
merge_with_windowing()