from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, ArrayType, DoubleType
from pyspark.sql.functions import (
    explode_outer, col, when, lit, concat, coalesce
)

# -----------------------------
# 1. Initialize SparkSession
# -----------------------------

spark = SparkSession.builder \
    .appName("DataFrameReconciliation") \
    .config("spark.sql.shuffle.partitions", "400")  # Adjust based on your cluster
.getOrCreate()

# -----------------------------
# 2. Define Schemas Using StructType
# -----------------------------

# Define the schema for the 'address' struct
address_schema = StructType([
    StructField("street", StringType(), True),
    StructField("city", StringType(), True),
    StructField("zipcode", StringType(), True)
])

# Define the schema for the 'order' struct
order_schema = StructType([
    StructField("order_id", StringType(), True),
    StructField("order_date", StringType(), True),
    StructField("amount", DoubleType(), True),
    StructField("status", StringType(), True)
])

# Define the schema for the 'payment' struct
payment_schema = StructType([
    StructField("payment_id", StringType(), True),
    StructField("payment_date", StringType(), True),
    StructField("method", StringType(), True),
    StructField("amount", DoubleType(), True)
])

# Define the main schema for the DataFrames
main_schema = StructType([
    StructField("parent_primary_key", StringType(), True),
    StructField("child_primary_key", StringType(), True),
    StructField("name", StringType(), True),
    StructField("age", IntegerType(), True),
    StructField("address", address_schema, True),
    StructField("orders", ArrayType(order_schema), True),
    StructField("payments", ArrayType(payment_schema), True)
])

# -----------------------------
# 3. Create Sample DataFrames
# -----------------------------

# Sample data for source_df
source_data = [
    {
        "parent_primary_key": "P1",
        "child_primary_key": "C1",
        "name": "Alice",
        "age": 30,
        "address": {"street": "123 Maple St", "city": "Springfield", "zipcode": "12345"},
        "orders": [
            {"order_id": "O1001", "order_date": "2023-01-10", "amount": 250.0, "status": "Shipped"},
            {"order_id": "O1002", "order_date": "2023-02-15", "amount": 150.0, "status": "Processing"}
        ],
        "payments": [
            {"payment_id": "PM2001", "payment_date": "2023-01-11", "method": "Credit Card", "amount": 250.0}
        ]
    },
    {
        "parent_primary_key": "P2",
        "child_primary_key": "C2",
        "name": "Bob",
        "age": 25,
        "address": {"street": "456 Oak St", "city": "Shelbyville", "zipcode": "67890"},
        "orders": [
            {"order_id": "O1003", "order_date": "2023-03-20", "amount": 300.0, "status": "Delivered"}
        ],
        "payments": [
            {"payment_id": "PM2002", "payment_date": "2023-03-21", "method": "PayPal", "amount": 300.0}
        ]
    }
]

# Sample data for target_df with some differences
target_data = [
    {
        "parent_primary_key": "P1",
        "child_primary_key": "C1",
        "name": "Alice",
        "age": 31,  # Age difference
        "address": {"street": "123 Maple St", "city": "Springfield", "zipcode": "12345"},
        "orders": [
            {"order_id": "O1001", "order_date": "2023-01-10", "amount": 250.0, "status": "Shipped"},
            {"order_id": "O1002", "order_date": "2023-02-15", "amount": 175.0, "status": "Completed"}  # Amount and status difference
        ],
        "payments": [
            {"payment_id": "PM2001", "payment_date": "2023-01-11", "method": "Credit Card", "amount": 250.0}
        ]
    },
    {
        "parent_primary_key": "P2",
        "child_primary_key": "C2",
        "name": "Bob",
        "age": 25,
        "address": {"street": "456 Oak St", "city": "Shelbyville", "zipcode": "67890"},
        "orders": [
            {"order_id": "O1003", "order_date": "2023-03-20", "amount": 300.0, "status": "Delivered"}
        ],
        "payments": [
            {"payment_id": "PM2002", "payment_date": "2023-03-21", "method": "Credit Card", "amount": 300.0}  # Payment method difference
        ]
    },
    {
        "parent_primary_key": "P3",
        "child_primary_key": "C3",
        "name": "Charlie",
        "age": 28,
        "address": {"street": "789 Pine St", "city": "Capital City", "zipcode": "54321"},
        "orders": [],
        "payments": []
    }
]

# Create DataFrames with the defined schemas
source_df = spark.createDataFrame(source_data, schema=main_schema)
target_df = spark.createDataFrame(target_data, schema=main_schema)

# -----------------------------
# 4. Flatten Array of Structs to One Level
# -----------------------------

def explode_array_columns(df, array_columns, prefix):
    """
    Explode array of structs to one level without flattening internal attributes.

    :param df: Input DataFrame
    :param array_columns: List of array column names to explode
    :param prefix: Prefix to identify source or target
    :return: Flattened DataFrame
    """
    for array_col in array_columns:
        df = df.withColumn(f"{prefix}{array_col[:-1]}", explode_outer(col(array_col)))  # e.g., 'orders' -> 'order'
    return df

# Explode 'orders' and 'payments' arrays in source and target DataFrames
source_exploded = explode_array_columns(source_df, ['orders', 'payments'], 'source_')
target_exploded = explode_array_columns(target_df, ['orders', 'payments'], 'target_')

# -----------------------------
# 5. Add Prefixes to Non-Primary Key Columns
# -----------------------------

def add_prefix_to_columns(df, prefix, exclude_columns):
    """
    Add a prefix to all columns except the specified ones.

    :param df: Input DataFrame
    :param prefix: Prefix string (e.g., 'source_', 'target_')
    :param exclude_columns: List of column names to exclude from prefixing
    :return: DataFrame with prefixed column names
    """
    return df.select([
        col(column).alias(column) if column in exclude_columns else col(column).alias(f"{prefix}{column}")
        for column in df.columns
    ])

# Define primary keys
primary_keys = ['parent_primary_key', 'child_primary_key']

# Add prefixes to source and target DataFrames
source_exploded_prefixed = add_prefix_to_columns(source_exploded, 'source_', primary_keys + ['source_orders', 'source_payments'])
target_exploded_prefixed = add_prefix_to_columns(target_exploded, 'target_', primary_keys + ['target_orders', 'target_payments'])

# -----------------------------
# 6. Join DataFrames on Primary Keys and Unique Identifiers
# -----------------------------

def join_exploded_dfs(source_df, target_df, unique_keys):
    """
    Join source and target exploded DataFrames on primary keys and unique identifiers.

    :param source_df: Source exploded DataFrame with prefixed columns
    :param target_df: Target exploded DataFrame with prefixed columns
    :param unique_keys: List of unique key columns to join on
    :return: Joined DataFrame
    """
    join_condition = True
    for key in unique_keys:
        source_key = f"source_{key}"
        target_key = f"target_{key}"
        join_condition = join_condition & (col(source_key) == col(target_key))

    joined_df = source_df.join(target_df, on=join_condition, how="full_outer")
    return joined_df

# Define unique keys for 'orders' and 'payments'
orders_unique_keys = ['parent_primary_key', 'child_primary_key', 'order_id']
payments_unique_keys = ['parent_primary_key', 'child_primary_key', 'payment_id']

# Join Orders
joined_orders = join_exploded_dfs(
    source_exploded_prefixed.select(primary_keys + [col for col in source_exploded_prefixed.columns if 'order_' in col]),
    target_exploded_prefixed.select(primary_keys + [col for col in target_exploded_prefixed.columns if 'order_' in col]),
    ['parent_primary_key', 'child_primary_key', 'order_id']
)

# Join Payments
joined_payments = join_exploded_dfs(
    source_exploded_prefixed.select(primary_keys + [col for col in source_exploded_prefixed.columns if 'payment_' in col]),
    target_exploded_prefixed.select(primary_keys + [col for col in target_exploded_prefixed.columns if 'payment_' in col]),
    ['parent_primary_key', 'child_primary_key', 'payment_id']
)

# -----------------------------
# 7. Compare Columns and Generate Differences
# -----------------------------

def compare_fields(joined_df, fields, prefix_source, prefix_target):
    """
    Compare source and target fields and generate difference descriptions.

    :param joined_df: Joined DataFrame with prefixed columns
    :param fields: List of fields to compare
    :param prefix_source: Prefix for source fields
    :param prefix_target: Prefix for target fields
    :return: DataFrame with difference descriptions
    """
    diff_expressions = []
    for field in fields:
        source_field = f"{prefix_source}{field}"
        target_field = f"{prefix_target}{field}"
        diff_col = when(
            ~col(source_field).eqNullSafe(col(target_field)),
            concat(
                lit(f"{field}: Source = "),
                coalesce(col(source_field).cast("string"), lit("null")),
                lit(", Target = "),
                coalesce(col(target_field).cast("string"), lit("null"))
            )
        ).alias(f"{field}_diff")
        diff_expressions.append(diff_col)

    # Select primary keys and difference columns
    result_df = joined_df.select(
        'parent_primary_key',
        'child_primary_key',
        *diff_expressions
    )

    return result_df

# Compare Orders Fields
orders_fields = ['order_date', 'amount', 'status']
orders_diff = compare_fields(
    joined_orders,
    orders_fields,
    'source_order_',
    'target_order_'
)

# Compare Payments Fields
payments_fields = ['payment_date', 'method', 'amount']
payments_diff = compare_fields(
    joined_payments,
    payments_fields,
    'source_payment_',
    'target_payment_'
)

# -----------------------------
# 8. Display and Save Results
# -----------------------------

# Display Orders Differences
print("=== Orders Differences ===")
orders_diff.show(truncate=False)

# Display Payments Differences
print("=== Payments Differences ===")
payments_diff.show(truncate=False)

# Optionally, save the differences to storage (e.g., Parquet files)
# orders_diff.write.mode("overwrite").parquet("/path/to/orders_differences.parquet")
# payments_diff.write.mode("overwrite").parquet("/path/to/payments_differences.parquet")
