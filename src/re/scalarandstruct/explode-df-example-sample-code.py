from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, ArrayType, DoubleType
)
from pyspark.sql.functions import (
    explode_outer, col, when, lit, concat, coalesce
)

# Initialize SparkSession
spark = SparkSession.builder \
    .appName("DataFrameReconciliation") \
    .config("spark.sql.shuffle.partitions", "400")  # Adjust based on your cluster
.getOrCreate()

# Define Schemas
# [Same as your provided code]

# Create Sample DataFrames
# [Same as your provided code]

# Explode Array of Structs to One Level
def explode_array_columns(df, array_columns, prefix):
    for array_col in array_columns:
        df = df.withColumn(f"{prefix}{array_col[:-1]}", explode_outer(col(array_col)))
    return df

source_exploded = explode_array_columns(source_df, ['orders', 'payments'], 'source_')
target_exploded = explode_array_columns(target_df, ['orders', 'payments'], 'target_')

# Add Prefixes to Non-Primary Key Columns
def add_prefix_to_columns(df, prefix, exclude_columns):
    return df.select([
        col(column).alias(column) if column in exclude_columns else col(column).alias(f"{prefix}{column}")
        for column in df.columns
    ])

primary_keys = ['parent_primary_key', 'child_primary_key']
order_unique_keys = ['parent_primary_key', 'child_primary_key', 'order_id']
payment_unique_keys = ['parent_primary_key', 'child_primary_key', 'payment_id']

source_exploded_prefixed = add_prefix_to_columns(
    source_exploded,
    'source_',
    primary_keys + ['source_order', 'source_payment']
)

target_exploded_prefixed = add_prefix_to_columns(
    target_exploded,
    'target_',
    primary_keys + ['target_order', 'target_payment']
)

# Corrected join_exploded_dfs Function
def join_exploded_dfs(source_df, target_df, unique_keys):
    join_condition = None
    for key in unique_keys:
        source_key = f"source_{key}"
        target_key = f"target_{key}"
        condition = col(source_key).eqNullSafe(col(target_key))
        if join_condition is None:
            join_condition = condition
        else:
            join_condition = join_condition & condition

    joined_df = source_df.join(target_df, on=join_condition, how="full_outer")
    return joined_df

# Join Orders
joined_orders = join_exploded_dfs(
    source_exploded_prefixed.select(
        primary_keys + ['source_order.*']
    ),
    target_exploded_prefixed.select(
        primary_keys + ['target_order.*']
    ),
    order_unique_keys
)

# Join Payments
joined_payments = join_exploded_dfs(
    source_exploded_prefixed.select(
        primary_keys + ['source_payment.*']
    ),
    target_exploded_prefixed.select(
        primary_keys + ['target_payment.*']
    ),
    payment_unique_keys
)

# Compare Columns and Generate Differences
def compare_fields(joined_df, fields, prefix_source, prefix_target):
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

    result_df = joined_df.select(
        *primary_keys,
        *diff_expressions
    )
    return result_df

# Compare Orders Fields
orders_fields = ['order_date', 'amount', 'status']
orders_diff = compare_fields(
    joined_orders,
    orders_fields,
    '',
    ''
)

# Compare Payments Fields
payments_fields = ['payment_date', 'method', 'amount']
payments_diff = compare_fields(
    joined_payments,
    payments_fields,
    '',
    ''
)

# Display Results
print("=== Orders Differences ===")
orders_diff.show(truncate=False)

print("=== Payments Differences ===")
payments_diff.show(truncate=False)
