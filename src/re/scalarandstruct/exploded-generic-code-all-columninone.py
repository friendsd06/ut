from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, ArrayType, DoubleType
)
from pyspark.sql.functions import (
    explode_outer, col, when, lit, concat, coalesce, array, concat_ws
)
from functools import reduce

# -----------------------------
# 1. Initialize SparkSession
# -----------------------------

spark = SparkSession.builder \
    .appName("GenericDataFrameReconciliation") \
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
    StructField("payments", ArrayType(payment_schema), True),
    # Add more array columns as needed
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
# 4. Generic Functions to Process Any Number of Array Columns
# -----------------------------

def explode_and_prefix_multiple(df, array_columns, prefix, primary_keys):
    """
    Explode multiple array columns and prefix the resulting fields.

    :param df: Input DataFrame
    :param array_columns: List of array columns to explode
    :param prefix: Prefix to add to the fields
    :param primary_keys: List of primary key columns
    :return: Dictionary of DataFrames with keys as array column names
    """
    exploded_dfs = {}
    for array_column in array_columns:
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

        exploded_dfs[array_column] = result_df

    return exploded_dfs

def join_exploded_dfs(source_df_dict, target_df_dict, array_columns, primary_keys):
    """
    Join source and target exploded DataFrames on primary keys and unique identifiers.

    :param source_df_dict: Dictionary of source DataFrames keyed by array column names
    :param target_df_dict: Dictionary of target DataFrames keyed by array column names
    :param array_columns: List of array columns
    :param primary_keys: List of primary key columns
    :return: Dictionary of joined DataFrames
    """
    joined_dfs = {}
    for array_column in array_columns:
        source_df = source_df_dict[array_column]
        target_df = target_df_dict[array_column]

        # Rename primary keys in target_df to avoid ambiguity
        for key in primary_keys:
            target_df = target_df.withColumnRenamed(key, f"{key}_target")

        # Get unique identifier keys
        struct_fields = [col_name for col_name in source_df.columns if col_name not in primary_keys]
        id_fields = [col_name for col_name in struct_fields if col_name.endswith('_id')]

        source_keys = primary_keys + id_fields
        target_keys = [key if key not in primary_keys else f"{key}_target" for key in source_keys]

        # Build join conditions
        join_condition = reduce(lambda x, y: x & y, [
            col(s_key).eqNullSafe(col(t_key))
            for s_key, t_key in zip(source_keys, target_keys)
        ])

        joined_df = source_df.join(target_df, on=join_condition, how="full_outer")

        joined_dfs[array_column] = joined_df

    return joined_dfs

def compare_fields_generic(joined_df, fields, source_prefix, target_prefix, primary_keys, array_column):
    """
    Compare source and target fields and generate a single difference description per row.

    :param joined_df: Joined DataFrame with prefixed columns
    :param fields: List of fields to compare (without prefixes)
    :param source_prefix: Prefix for source fields
    :param target_prefix: Prefix for target fields
    :param primary_keys: List of primary key columns
    :param array_column: Name of the array column being processed
    :return: DataFrame with combined difference descriptions
    """
    diff_expressions = []
    for field in fields:
        source_field = f"{source_prefix}{field}"
        target_field = f"{target_prefix}{field}"
        diff_expr = when(
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
        )
        diff_expressions.append(diff_expr)

    # Combine all differences into one column
    combined_diff = concat_ws("; ", array(*diff_expressions))

    # Include the original primary keys
    result_df = joined_df.select(
        *[col(key) if key in joined_df.columns else col(f"{key}_target").alias(key) for key in primary_keys],
        combined_diff.alias(f"{array_column}_differences")
    ).where(col(f"{array_column}_differences").isNotNull())

    return result_df

# -----------------------------
# 5. Process Multiple Array Columns
# -----------------------------

# Define primary keys
primary_keys = ['parent_primary_key', 'child_primary_key']

# Define array columns to process
array_columns = ['orders', 'payments']  # Add more array columns as needed

# Explode and prefix arrays in source and target DataFrames
source_exploded_dfs = explode_and_prefix_multiple(source_df, array_columns, 'source_', primary_keys)
target_exploded_dfs = explode_and_prefix_multiple(target_df, array_columns, 'target_', primary_keys)

# Join exploded DataFrames
joined_dfs = join_exploded_dfs(source_exploded_dfs, target_exploded_dfs, array_columns, primary_keys)

# -----------------------------
# 6. Compare Columns and Generate Differences
# -----------------------------

# Define fields to compare for each array column
fields_to_compare = {
    'orders': ['order_date', 'amount', 'status'],
    'payments': ['payment_date', 'method', 'amount'],
    # Add more array columns and their fields as needed
}

diff_results = {}
for array_column in array_columns:
    joined_df = joined_dfs[array_column]
    fields = fields_to_compare[array_column]
    diff_df = compare_fields_generic(
        joined_df,
        fields,
        'source_',
        'target_',
        primary_keys,
        array_column
    )
    diff_results[array_column] = diff_df

# -----------------------------
# 7. Display Results
# -----------------------------

for array_column in array_columns:
    print(f"=== Differences in {array_column.capitalize()} ===")
    diff_results[array_column].show(truncate=False)
