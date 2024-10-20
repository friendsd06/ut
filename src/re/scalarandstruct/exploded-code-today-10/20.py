from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, ArrayType, DoubleType
)
from pyspark.sql.functions import (
    explode_outer, col, when, lit, concat, coalesce, array, concat_ws
)
from functools import reduce

# ------------------------------------
# 1. Initialize SparkSession
# ------------------------------------

spark = SparkSession.builder \
    .appName("DataFrameReconciliation") \
    .getOrCreate()

# ------------------------------------
# 2. Define Schemas
# ------------------------------------

# Define schema for 'address' struct
address_schema = StructType([
    StructField("street", StringType()),
    StructField("city", StringType()),
    StructField("zipcode", StringType())
])

# Define schema for 'order' struct
order_schema = StructType([
    StructField("order_id", StringType()),
    StructField("order_date", StringType()),
    StructField("amount", DoubleType()),
    StructField("status", StringType())
])

# Define schema for 'payment' struct
payment_schema = StructType([
    StructField("payment_id", StringType()),
    StructField("payment_date", StringType()),
    StructField("method", StringType()),
    StructField("amount", DoubleType())
])

# Define schema for 'new_array' struct with multiple primary keys
new_array_schema = StructType([
    StructField("new_id1", StringType()),
    StructField("new_id2", StringType()),
    StructField("detail", StringType())
])

# Define main schema combining all fields
main_schema = StructType([
    StructField("parent_primary_key", StringType()),
    StructField("child_primary_key", StringType()),
    StructField("name", StringType()),
    StructField("age", IntegerType()),
    StructField("address", address_schema),
    StructField("orders", ArrayType(order_schema)),
    StructField("payments", ArrayType(payment_schema)),
    StructField("new_array", ArrayType(new_array_schema))  # Added new_array
])

# ------------------------------------
# 3. Create Sample DataFrames
# ------------------------------------

# Sample data for source DataFrame
source_data = [
    {
        "parent_primary_key": "P1",
        "child_primary_key": "C1",
        "name": "Alice",
        "age": 30,
        "address": {
            "street": "123 Maple St",
            "city": "Springfield",
            "zipcode": "12345"
        },
        "orders": [
            {"order_id": "O1001", "order_date": "2023-01-10", "amount": 250.0, "status": "Shipped"},
            {"order_id": "O1002", "order_date": "2023-02-15", "amount": 150.0, "status": "Processing"}
        ],
        "payments": [
            {"payment_id": "PM2001", "payment_date": "2023-01-11", "method": "Credit Card", "amount": 250.0}
        ],
        "new_array": [
            {"new_id1": "N3001", "new_id2": "N4001", "detail": "Detail A"},
            {"new_id1": "N3002", "new_id2": "N4002", "detail": "Detail B"}
        ]
    },
    {
        "parent_primary_key": "P2",
        "child_primary_key": "C2",
        "name": "Bob",
        "age": 25,
        "address": {
            "street": "456 Oak St",
            "city": "Shelbyville",
            "zipcode": "67890"
        },
        "orders": [
            {"order_id": "O1003", "order_date": "2023-03-20", "amount": 300.0, "status": "Delivered"}
        ],
        "payments": [
            {"payment_id": "PM2002", "payment_date": "2023-03-21", "method": "PayPal", "amount": 300.0}
        ],
        "new_array": [
            {"new_id1": "N3003", "new_id2": "N4003", "detail": "Detail C"}
        ]
    }
]

# Sample data for target DataFrame with some differences
target_data = [
    {
        "parent_primary_key": "P1",
        "child_primary_key": "C1",
        "name": "Alice",
        "age": 31,  # Age difference
        "address": {
            "street": "123 Maple St",
            "city": "Springfield",
            "zipcode": "12345"
        },
        "orders": [
            {"order_id": "O1001", "order_date": "2023-01-10", "amount": 250.0, "status": "Shipped"},
            {"order_id": "O1002", "order_date": "2023-02-15", "amount": 175.0, "status": "Completed"}  # Differences here
        ],
        "payments": [
            {"payment_id": "PM2001", "payment_date": "2023-01-11", "method": "Credit Card", "amount": 250.0}
        ],
        "new_array": [
            {"new_id1": "N3001", "new_id2": "N4001", "detail": "Detail A"},
            {"new_id1": "N3002", "new_id2": "N4002", "detail": "Detail B Modified"}  # Difference here
        ]
    },
    {
        "parent_primary_key": "P2",
        "child_primary_key": "C2",
        "name": "Bob",
        "age": 25,
        "address": {
            "street": "456 Oak St",
            "city": "Shelbyville",
            "zipcode": "67890"
        },
        "orders": [
            {"order_id": "O1003", "order_date": "2023-03-20", "amount": 300.0, "status": "Delivered"}
        ],
        "payments": [
            {"payment_id": "PM2002", "payment_date": "2023-03-21", "method": "Credit Card", "amount": 300.0}  # Difference here
        ],
        "new_array": [
            {"new_id1": "N3003", "new_id2": "N4003", "detail": "Detail C"},
            {"new_id1": "N3004", "new_id2": "N4004", "detail": "Detail D"}  # New entry
        ]
    },
    {
        "parent_primary_key": "P3",
        "child_primary_key": "C3",
        "name": "Charlie",
        "age": 28,
        "address": {
            "street": "789 Pine St",
            "city": "Capital City",
            "zipcode": "54321"
        },
        "orders": [],
        "payments": [],
        "new_array": []  # No entries
    }
]

# Create DataFrames with the defined schemas
source_df = spark.createDataFrame(source_data, main_schema)
target_df = spark.createDataFrame(target_data, main_schema)

# ------------------------------------
# 4. Functions to Process Array Columns
# ------------------------------------

def explode_and_prefix(df, array_columns, prefix, primary_keys):
    """
    Explode specified array columns and prefix the resulting fields.

    Parameters:
        df (DataFrame): The input DataFrame.
        array_columns (dict): Dictionary mapping array column names to their primary keys.
        prefix (str): Prefix to add to the exploded fields.
        primary_keys (list): List of primary key column names.

    Returns:
        dict: Dictionary of DataFrames with exploded and prefixed fields.
    """
    exploded_dfs = {}
    for col_name, pk in array_columns.items():
        # Explode the array column
        exploded_col = df.withColumn(f"{col_name}_exploded", explode_outer(col(col_name)))

        # Select primary keys and exploded struct fields
        struct_fields = exploded_col.select(f"{col_name}_exploded.*").columns
        prefixed_fields = [col(f"{col_name}_exploded.{field}").alias(f"{prefix}{field}") for field in struct_fields]
        selected_cols = primary_keys + prefixed_fields
        exploded_df = exploded_col.select(*selected_cols)
        exploded_dfs[col_name] = exploded_df
    return exploded_dfs

def join_exploded_dfs(source_dfs, target_dfs, array_columns, primary_keys):
    """
    Join source and target exploded DataFrames on primary keys and unique identifiers.

    Parameters:
        source_dfs (dict): Dictionary of source DataFrames keyed by array column names.
        target_dfs (dict): Dictionary of target DataFrames keyed by array column names.
        array_columns (dict): Dictionary mapping array column names to their primary keys.
        primary_keys (list): List of primary key column names.

    Returns:
        dict: Dictionary of joined DataFrames.
    """
    joined_dfs = {}
    for col_name, pk in array_columns.items():
        source_df = source_dfs[col_name]
        target_df = target_dfs[col_name]

        # Rename primary keys in target_df to avoid ambiguity
        for key in primary_keys:
            target_df = target_df.withColumnRenamed(key, f"{key}_target")
        for key in pk:
            target_df = target_df.withColumnRenamed(key, f"{key}_target")

        # Identify join keys: global primary keys + array-specific primary keys
        source_join_keys = primary_keys + [f"{key}" for key in pk]
        target_join_keys = [f"{key}_target" for key in primary_keys] + [f"{key}_target" for key in pk]

        # Build join condition
        join_conditions = [
            source_df[pk_col] == target_df[pk_target_col]
            for pk_col, pk_target_col in zip(source_join_keys, target_join_keys)
        ]

        join_condition = reduce(lambda x, y: x & y, join_conditions)

        # Perform the join
        joined_df = source_df.join(target_df, on=join_condition, how="full_outer")
        joined_dfs[col_name] = joined_df
    return joined_dfs

def compare_and_combine_differences(joined_df, compare_fields, source_prefix, target_prefix, primary_keys, array_specific_pks, result_col_name):
    """
    Compare fields between source and target DataFrames and combine differences into a single column.

    Parameters:
        joined_df (DataFrame): The joined DataFrame.
        compare_fields (list or None): List of field names to compare. If None or empty, compare all fields.
        source_prefix (str): Prefix of source fields.
        target_prefix (str): Prefix of target fields.
        primary_keys (list): List of global primary key column names.
        array_specific_pks (list): List of array-specific primary key column names.
        result_col_name (str): Name of the result column for combined differences.

    Returns:
        DataFrame: DataFrame with primary keys and combined differences.
    """
    # If compare_fields is None or empty, compare all fields except IDs and primary keys
    if not compare_fields:
        # Extract all fields from source prefix
        all_source_fields = [
            col_name[len(source_prefix):] for col_name in joined_df.columns
            if col_name.startswith(source_prefix) and not any(col_name[len(source_prefix):].startswith(pk) for pk in primary_keys + array_specific_pks)
        ]
        compare_fields = all_source_fields

    difference_expressions = []
    for field in compare_fields:
        source_field = f"{source_prefix}{field}"
        target_field = f"{target_prefix}{field}"
        # Create expressions to capture differences
        diff_expr = when(
            col(source_field).isNull() & col(target_field).isNotNull(),
            concat(lit(f"{field}: Source = null"), lit(", Target = "), col(target_field).cast("string"))
        ).when(
            col(source_field).isNotNull() & col(target_field).isNull(),
            concat(lit(f"{field}: Source = "), col(source_field).cast("string"), lit(", Target = null"))
        ).when(
            ~col(source_field).eqNullSafe(col(target_field)),
            concat(
                lit(f"{field}: Source = "), coalesce(col(source_field).cast("string"), lit("null")),
                lit(", Target = "), coalesce(col(target_field).cast("string"), lit("null"))
            )
        )
        difference_expressions.append(diff_expr)

    # Combine all differences into a single column, filtering out nulls
    combined_differences = concat_ws("; ", array(*difference_expressions))

    # Select primary keys and combined differences
    result_df = joined_df.select(
        *[
            col(key) if key in joined_df.columns else col(f"{key}_target").alias(key)
            for key in primary_keys
        ],
        *[
            col(key) if key in joined_df.columns else col(f"{key}_target").alias(key)
            for key in array_specific_pks
        ],
        combined_differences.alias(result_col_name)
    ).filter(col(result_col_name).isNotNull())

    return result_df

# ------------------------------------
# 5. Process and Compare Array Columns
# ------------------------------------

# Define primary keys
primary_keys = ["parent_primary_key", "child_primary_key"]

# Define array columns to process with their respective primary keys
array_columns = {
    "orders": ["order_id"],
    "payments": ["payment_id"],
    "new_array": ["new_id1", "new_id2"]  # Example of an array with multiple primary keys
}

# Explode and prefix arrays in source and target DataFrames
source_exploded_dfs = explode_and_prefix(source_df, array_columns, "source_", primary_keys)
target_exploded_dfs = explode_and_prefix(target_df, array_columns, "target_", primary_keys)

# Join exploded DataFrames
joined_dfs = join_exploded_dfs(source_exploded_dfs, target_exploded_dfs, array_columns, primary_keys)

# Define fields to compare for each array column (optional)
fields_to_compare = {
    "orders": ["order_date", "amount", "status"],
    "payments": ["payment_date", "method", "amount"],
    # "new_array" will compare all fields since it's not specified
}

# Compare fields and combine differences
difference_results = {}
for col_name, pk in array_columns.items():
    if col_name not in joined_dfs:
        continue  # Skip if no joined DataFrame for the column
    joined_df = joined_dfs[col_name]
    compare_fields = fields_to_compare.get(col_name)  # Will be None if not specified
    result_col_name = f"{col_name}_differences"
    diff_df = compare_and_combine_differences(
        joined_df,
        compare_fields,
        "source_",
        "target_",
        primary_keys,
        pk,  # Array-specific primary keys
        result_col_name
    )
    difference_results[col_name] = diff_df

# ------------------------------------
# 6. Display Results
# ------------------------------------

for col_name in array_columns.keys():
    print(f"=== Differences in {col_name.capitalize()} ===")
    if col_name in difference_results:
        difference_results[col_name].show(truncate=False)
    else:
        print("No differences found.")

# Stop the SparkSession when done
spark.stop()
