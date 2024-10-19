from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, ArrayType, DoubleType
)
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

# Schema for the 'address' struct
address_schema = StructType([
    StructField("street", StringType(), True),
    StructField("city", StringType(), True),
    StructField("zipcode", StringType(), True)
])

# Schema for the 'order' struct
order_schema = StructType([
    StructField("order_id", StringType(), True),
    StructField("order_date", StringType(), True),
    StructField("amount", DoubleType(), True),
    StructField("status", StringType(), True)
])

# Schema for the 'payment' struct
payment_schema = StructType([
    StructField("payment_id", StringType(), True),
    StructField("payment_date", StringType(), True),
    StructField("method", StringType(), True),
    StructField("amount", DoubleType(), True)
])

# Main schema combining all fields
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
# 4. Flatten Array of Structs to One Level and Prefix Fields
# -----------------------------

def explode_and_prefix(df, array_column, prefix, primary_keys):
    """
    Explode an array of structs and prefix the resulting fields.

    :param df: Input DataFrame
    :param array_column: Name of the array column to explode
    :param prefix: Prefix to add to the fields
    :param primary_keys: List of primary key columns
    :return: Flattened and prefixed DataFrame
    """
    exploded_df = df.withColumn(f"{array_column}_exploded", explode_outer(col(array_column))) \
        .drop(array_column)

    # Flatten the struct fields and add prefix
    struct_fields = exploded_df.select(f"{array_column}_exploded.*").columns
    prefixed_fields = [col(f"{array_column}_exploded.{field}").alias(f"{prefix}{field}") for field in struct_fields]

    # Select primary keys and prefixed fields
    result_df = exploded_df.select(
        *primary_keys,
        *prefixed_fields
    )

    return result_df

# Define primary keys
primary_keys = ['parent_primary_key', 'child_primary_key']

# Explode and prefix 'orders' and 'payments' in source DataFrame
source_orders = explode_and_prefix(source_df, 'orders', 'source_', primary_keys)
source_payments = explode_and_prefix(source_df, 'payments', 'source_', primary_keys)

# Explode and prefix 'orders' and 'payments' in target DataFrame
target_orders = explode_and_prefix(target_df, 'orders', 'target_', primary_keys)
target_payments = explode_and_prefix(target_df, 'payments', 'target_', primary_keys)

# -----------------------------
# 5. Join DataFrames on Primary Keys and Unique Identifiers
# -----------------------------

def join_exploded_dfs(source_df, target_df, source_keys, target_keys, primary_keys):
    """
    Join source and target exploded DataFrames on primary keys and unique identifiers.

    :param source_df: Source DataFrame with prefixed columns
    :param target_df: Target DataFrame with prefixed columns
    :param source_keys: List of source key columns
    :param target_keys: List of target key columns
    :param primary_keys: List of primary key columns
    :return: Joined DataFrame
    """
    # Rename primary keys in target_df to avoid ambiguity
    for key in primary_keys:
        target_df = target_df.withColumnRenamed(key, f"{key}_target")

    # Update target keys with renamed columns
    target_keys_renamed = [f"{key}_target" if key in primary_keys else key for key in target_keys]

    # Build join conditions
    join_condition = None
    for s_key, t_key in zip(source_keys, target_keys_renamed):
        condition = col(s_key).eqNullSafe(col(t_key))
        if join_condition is None:
            join_condition = condition
        else:
            join_condition = join_condition & condition

    joined_df = source_df.join(target_df, on=join_condition, how="full_outer")
    return joined_df

# Define unique keys for orders
source_order_keys = ['parent_primary_key', 'child_primary_key', 'source_order_id']
target_order_keys = ['parent_primary_key', 'child_primary_key', 'target_order_id']

# Join Orders
joined_orders = join_exploded_dfs(
    source_orders,
    target_orders,
    source_order_keys,
    target_order_keys,
    primary_keys
)

# Define unique keys for payments
source_payment_keys = ['parent_primary_key', 'child_primary_key', 'source_payment_id']
target_payment_keys = ['parent_primary_key', 'child_primary_key', 'target_payment_id']

# Join Payments
joined_payments = join_exploded_dfs(
    source_payments,
    target_payments,
    source_payment_keys,
    target_payment_keys,
    primary_keys
)

# -----------------------------
# 6. Compare Columns and Generate Differences
# -----------------------------

def compare_fields(joined_df, fields, source_prefix, target_prefix, primary_keys):
    """
    Compare source and target fields and generate difference descriptions.

    :param joined_df: Joined DataFrame with prefixed columns
    :param fields: List of fields to compare (without prefixes)
    :param source_prefix: Prefix for source fields
    :param target_prefix: Prefix for target fields
    :param primary_keys: List of primary key columns
    :return: DataFrame with difference descriptions
    """
    diff_expressions = []
    for field in fields:
        source_field = f"{source_prefix}{field}"
        target_field = f"{target_prefix}{field}"
        diff_col = when(
            col(source_field).isNull() & col(target_field).isNotNull(),
            concat(
                lit(f"{field}: Source = null"),
                lit(", Target = "),
                col(target_field).cast("string")
            )
        ).when(
            col(source_field).isNotNull() & col(target_field).isNull(),
            concat(
                lit(f"{field}: Source = "),
                col(source_field).cast("string"),
                lit(", Target = null")
            )
        ).when(
            ~col(source_field).eqNullSafe(col(target_field)),
            concat(
                lit(f"{field}: Source = "),
                coalesce(col(source_field).cast("string"), lit("null")),
                lit(", Target = "),
                coalesce(col(target_field).cast("string"), lit("null"))
            )
        ).alias(f"{field}_diff")
        diff_expressions.append(diff_col)

    # Include the original primary keys from source_df
    result_df = joined_df.select(
        *[col(key) for key in primary_keys],
        *diff_expressions
    )
    return result_df

# Compare Orders Fields
orders_fields = ['order_date', 'amount', 'status']
orders_diff = compare_fields(
    joined_orders,
    orders_fields,
    'source_',
    'target_',
    primary_keys
)

# Compare Payments Fields
payments_fields = ['payment_date', 'method', 'amount']
payments_diff = compare_fields(
    joined_payments,
    payments_fields,
    'source_',
    'target_',
    primary_keys
)

# -----------------------------
# 7. Display Results
# -----------------------------

print("=== Orders Differences ===")
orders_diff.show(truncate=False)

print("=== Payments Differences ===")
payments_diff.show(truncate=False)
