from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, ArrayType, DoubleType
from pyspark.sql.functions import (
    explode_outer, col, when, lit, concat, coalesce, expr
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

# Function to explode array of structs to one level
def explode_array_of_structs(df, array_columns):
    """
    Explode array of structs to one level without flattening the struct attributes.

    :param df: Input DataFrame
    :param array_columns: List of array column names to explode
    :return: Dictionary of exploded DataFrames
    """
    exploded_dfs = {}
    for array_col in array_columns:
        exploded_df = df.select("*", explode_outer(array_col).alias(array_col.rstrip('s')))  # e.g., 'orders' -> 'order'
        exploded_dfs[array_col] = exploded_df
    return exploded_dfs

# Explode 'orders' and 'payments' arrays in both source and target DataFrames
source_exploded = explode_array_of_structs(source_df, ['orders', 'payments'])
target_exploded = explode_array_of_structs(target_df, ['orders', 'payments'])

# For clarity, create separate DataFrames for orders and payments
source_orders = source_exploded['orders'].select(
    'parent_primary_key',
    'child_primary_key',
    'order_id',
    'order_date',
    'amount',
    'status'
)

source_payments = source_exploded['payments'].select(
    'parent_primary_key',
    'child_primary_key',
    'payment_id',
    'payment_date',
    'method',
    'amount'
)

target_orders = target_exploded['orders'].select(
    'parent_primary_key',
    'child_primary_key',
    'order_id',
    'order_date',
    'amount',
    'status'
)

target_payments = target_exploded['payments'].select(
    'parent_primary_key',
    'child_primary_key',
    'payment_id',
    'payment_date',
    'method',
    'amount'
)

# -----------------------------
# 5. Add Prefixes to Non-Primary Key Columns
# -----------------------------

def add_prefix(df, prefix, unique_key_cols):
    """
    Add a prefix to all columns except the unique key columns.

    :param df: Input DataFrame
    :param prefix: Prefix string (e.g., 'source_', 'target_')
    :param unique_key_cols: List of columns to exclude from prefixing
    :return: DataFrame with prefixed column names
    """
    return df.select([
        col(column).alias(column) if column in unique_key_cols else col(column).alias(f"{prefix}{column}")
        for column in df.columns
    ])

# Define unique keys for orders and payments
order_unique_keys = ['parent_primary_key', 'child_primary_key', 'order_id']
payment_unique_keys = ['parent_primary_key', 'child_primary_key', 'payment_id']

# Add prefixes to source and target orders
source_orders_prefixed = add_prefix(source_orders, "source_", ['parent_primary_key', 'child_primary_key', 'order_id'])
target_orders_prefixed = add_prefix(target_orders, "target_", ['parent_primary_key', 'child_primary_key', 'order_id'])

# Add prefixes to source and target payments
source_payments_prefixed = add_prefix(source_payments, "source_", ['parent_primary_key', 'child_primary_key', 'payment_id'])
target_payments_prefixed = add_prefix(target_payments, "target_", ['parent_primary_key', 'child_primary_key', 'payment_id'])

# -----------------------------
# 6. Join DataFrames on Primary Keys and Unique Identifiers
# -----------------------------

# Function to join source and target exploded DataFrames on composite keys
def join_exploded_dfs(source_df, target_df, unique_key_cols, additional_keys):
    """
    Join source and target exploded DataFrames on composite keys.

    :param source_df: Source exploded DataFrame with prefixed columns
    :param target_df: Target exploded DataFrame with prefixed columns
    :param unique_key_cols: List of unique key columns to join on (e.g., ['parent_primary_key', 'child_primary_key', 'order_id'])
    :param additional_keys: List of additional key columns to handle differences
    :return: Joined DataFrame with comparison results
    """
    # Construct join condition
    join_condition = " AND ".join([
        f"source_{col} = target_{col}" for col in unique_key_cols
    ])

    joined_df = source_df.join(
        target_df,
        expr(join_condition),
        how="full_outer"
    )

    return joined_df

# Join orders
joined_orders = join_exploded_dfs(
    source_orders_prefixed,
    target_orders_prefixed,
    ['parent_primary_key', 'child_primary_key', 'order_id'],
    []
)

# Join payments
joined_payments = join_exploded_dfs(
    source_payments_prefixed,
    target_payments_prefixed,
    ['parent_primary_key', 'child_primary_key', 'payment_id'],
    []
)

# -----------------------------
# 7. Compare Columns and Generate Differences
# -----------------------------

def compare_fields(joined_df, fields):
    """
    Compare source and target fields and generate difference descriptions.

    :param joined_df: Joined DataFrame with prefixed columns
    :param fields: List of fields to compare
    :return: DataFrame with difference descriptions
    """
    diff_exprs = []
    for field in fields:
        source_field = f"source_{field}"
        target_field = f"target_{field}"
        diff_col = when(
            ~col(source_field).eqNullSafe(col(target_field)),
            concat(
                lit(f"{field}: Source = "),
                coalesce(col(source_field).cast("string"), lit("null")),
                lit(", Target = "),
                coalesce(col(target_field).cast("string"), lit("null"))
            )
        ).alias(f"{field}_diff")
        diff_exprs.append(diff_col)

    # Select unique keys and difference columns
    result_df = joined_df.select(
        'parent_primary_key',
        'child_primary_key',
        *diff_exprs
    )

    return result_df

# Compare Orders
orders_diff = compare_fields(joined_orders, ['order_date', 'amount', 'status'])

# Compare Payments
payments_diff = compare_fields(joined_payments, ['payment_date', 'method', 'amount'])

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
