from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, ArrayType, DoubleType
)
from pyspark.sql.functions import (
    explode_outer, col, when, lit, concat, coalesce, array, concat_ws
)
from functools import reduce
from datetime import datetime

# ------------------------------------
# 1. Initialize SparkSession
# ------------------------------------

spark = SparkSession.builder \
    .appName("DataFrameReconciliation") \
    .getOrCreate()

# ------------------------------------
# 2. Define Logging Function
# ------------------------------------

def log(message, level="INFO"):
    """
    Prints a log message with a timestamp and log level.

    Parameters:
        message (str): The log message.
        level (str): The log level (e.g., INFO, DEBUG, WARNING, ERROR, CRITICAL).
    """
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"{timestamp} - {level} - {message}")

# ------------------------------------
# 3. Define Schemas
# ------------------------------------

log("Defining schemas for DataFrames.", level="INFO")

# Schema for 'address' struct
address_schema = StructType([
    StructField("street", StringType(), True),
    StructField("city", StringType(), True),
    StructField("zipcode", StringType(), True)
])

# Schema for 'order' struct with foreign keys
order_schema = StructType([
    StructField("order_id", StringType(), True),          # Array-specific primary key
    StructField("parent_primary_key", StringType(), True), # Foreign key to parent
    StructField("child_primary_key", StringType(), True),  # Foreign key to parent
    StructField("order_date", StringType(), True),
    StructField("amount", DoubleType(), True),
    StructField("status", StringType(), True)
])

# Schema for 'payment' struct with foreign keys
payment_schema = StructType([
    StructField("payment_id", StringType(), True),         # Array-specific primary key
    StructField("parent_primary_key", StringType(), True), # Foreign key to parent
    StructField("child_primary_key", StringType(), True),  # Foreign key to parent
    StructField("payment_date", StringType(), True),
    StructField("method", StringType(), True),
    StructField("amount", DoubleType(), True)
])

# Schema for 'new_array' struct with multiple primary keys and foreign keys
new_array_schema = StructType([
    StructField("new_id1", StringType(), True),            # Part of composite primary key
    StructField("new_id2", StringType(), True),            # Part of composite primary key
    StructField("parent_primary_key", StringType(), True), # Foreign key to parent
    StructField("child_primary_key", StringType(), True),  # Foreign key to parent
    StructField("detail", StringType(), True)
])

# Main schema combining all fields
main_schema = StructType([
    StructField("parent_primary_key", StringType(), True), # Global primary key
    StructField("child_primary_key", StringType(), True),  # Global primary key
    StructField("name", StringType(), True),
    StructField("age", IntegerType(), True),
    StructField("address", address_schema, True),
    StructField("orders", ArrayType(order_schema), True),
    StructField("payments", ArrayType(payment_schema), True),
    StructField("new_array", ArrayType(new_array_schema), True)
])

# ------------------------------------
# 4. Create Sample DataFrames
# ------------------------------------

log("Creating sample source and target DataFrames.", level="INFO")

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
            {"order_id": "O1001", "parent_primary_key": "P1", "child_primary_key": "C1", "order_date": "2023-01-10", "amount": 250.0, "status": "Shipped"},
            {"order_id": "O1002", "parent_primary_key": "P1", "child_primary_key": "C1", "order_date": "2023-02-15", "amount": 150.0, "status": "Processing"}
        ],
        "payments": [
            {"payment_id": "PM2001", "parent_primary_key": "P1", "child_primary_key": "C1", "payment_date": "2023-01-11", "method": "Credit Card", "amount": 250.0}
        ],
        "new_array": [
            {"new_id1": "N3001", "new_id2": "N4001", "parent_primary_key": "P1", "child_primary_key": "C1", "detail": "Detail A"},
            {"new_id1": "N3002", "new_id2": "N4002", "parent_primary_key": "P1", "child_primary_key": "C1", "detail": "Detail B"}
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
            {"order_id": "O1003", "parent_primary_key": "P2", "child_primary_key": "C2", "order_date": "2023-03-20", "amount": 300.0, "status": "Delivered"}
        ],
        "payments": [
            {"payment_id": "PM2002", "parent_primary_key": "P2", "child_primary_key": "C2", "payment_date": "2023-03-21", "method": "PayPal", "amount": 300.0}
        ],
        "new_array": [
            {"new_id1": "N3003", "new_id2": "N4003", "parent_primary_key": "P2", "child_primary_key": "C2", "detail": "Detail C"}
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
            {"order_id": "O1001", "parent_primary_key": "P1", "child_primary_key": "C1", "order_date": "2023-01-10", "amount": 250.0, "status": "Shipped"},
            {"order_id": "O1002", "parent_primary_key": "P1", "child_primary_key": "C1", "order_date": "2023-02-15", "amount": 175.0, "status": "Completed"}  # Differences here
        ],
        "payments": [
            {"payment_id": "PM2001", "parent_primary_key": "P1", "child_primary_key": "C1", "payment_date": "2023-01-11", "method": "Credit Card", "amount": 250.0}
        ],
        "new_array": [
            {"new_id1": "N3001", "new_id2": "N4001", "parent_primary_key": "P1", "child_primary_key": "C1", "detail": "Detail A"},
            {"new_id1": "N3002", "new_id2": "N4002", "parent_primary_key": "P1", "child_primary_key": "C1", "detail": "Detail B Modified"}  # Difference here
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
            {"order_id": "O1003", "parent_primary_key": "P2", "child_primary_key": "C2", "order_date": "2023-03-20", "amount": 300.0, "status": "Delivered"}
        ],
        "payments": [
            {"payment_id": "PM2002", "parent_primary_key": "P2", "child_primary_key": "C2", "payment_date": "2023-03-21", "method": "Credit Card", "amount": 300.0}  # Difference here
        ],
        "new_array": [
            {"new_id1": "N3003", "new_id2": "N4003", "parent_primary_key": "P2", "child_primary_key": "C2", "detail": "Detail C"},
            {"new_id1": "N3004", "new_id2": "N4004", "parent_primary_key": "P2", "child_primary_key": "C2", "detail": "Detail D"}  # New entry
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

# Create DataFrames
log("Creating source and target DataFrames.", level="INFO")
source_df = spark.createDataFrame(source_data, main_schema)
target_df = spark.createDataFrame(target_data, main_schema)

# ------------------------------------
# 5. Define Processing Functions
# ------------------------------------

log("Defining processing functions.", level="INFO")

def explode_and_prefix(df, array_columns, prefix, global_primary_keys):
    """
    Explode specified array columns and prefix the resulting fields.

    Parameters:
        df (DataFrame): The input DataFrame.
        array_columns (dict): Dictionary mapping array column names to their array-specific primary keys.
        prefix (str): Prefix to add to the exploded fields (e.g., 'source_' or 'target_').
        global_primary_keys (list): List of global primary key column names.

    Returns:
        dict: Dictionary of exploded DataFrames keyed by array column names.
    """
    exploded_dfs = {}
    for array_col, array_pks in array_columns.items():
        log(f"Exploding and prefixing array column: {array_col}", level="DEBUG")
        # Explode the array column
        exploded_col = df.withColumn(f"{array_col}_exploded", explode_outer(col(array_col)))

        # Get all fields from the exploded array
        struct_fields = exploded_col.select(f"{array_col}_exploded.*").columns

        # Identify foreign keys (fields with the same names as global primary keys)
        foreign_keys = set(global_primary_keys) & set(struct_fields)

        # Select and prefix columns appropriately
        selected_cols = []
        for field in struct_fields:
            full_field = f"{array_col}_exploded.{field}"
            if field in array_pks:
                # Array-specific primary keys remain unprefixed
                selected_cols.append(col(full_field))
                log(f"Array-specific primary key: {field} (no prefix)", level="DEBUG")
            elif field in foreign_keys:
                # Prefix foreign keys with 'fk_'
                selected_cols.append(col(full_field).alias(f"fk_{field}"))
                log(f"Foreign key: {field} (prefixed with 'fk_')", level="DEBUG")
            else:
                # Prefix other fields
                selected_cols.append(col(full_field).alias(f"{prefix}{field}"))
                log(f"Field: {field} (prefixed with '{prefix}')", level="DEBUG")

        # Add global primary keys from parent DataFrame
        for pk in global_primary_keys:
            selected_cols.append(col(pk))
            log(f"Global primary key included: {pk}", level="DEBUG")

        # Create the exploded DataFrame
        exploded_df = exploded_col.select(*selected_cols)
        exploded_dfs[array_col] = exploded_df
        log(f"Exploded DataFrame for '{array_col}' created successfully.", level="INFO")

    return exploded_dfs

def join_exploded_dfs(source_dfs, target_dfs, array_columns, global_primary_keys):
    """
    Join source and target exploded DataFrames on global and array-specific primary keys.

    Parameters:
        source_dfs (dict): Dictionary of source exploded DataFrames keyed by array column names.
        target_dfs (dict): Dictionary of target exploded DataFrames keyed by array column names.
        array_columns (dict): Dictionary mapping array column names to their array-specific primary keys.
        global_primary_keys (list): List of global primary key column names.

    Returns:
        dict: Dictionary of joined DataFrames keyed by array column names.
    """
    joined_dfs = {}
    for array_col, array_pks in array_columns.items():
        log(f"Joining DataFrames for array column: {array_col}", level="DEBUG")
        source_df = source_dfs[array_col]
        target_df = target_dfs[array_col]

        # Build join keys: array-specific primary keys and foreign keys
        array_foreign_keys = [f"fk_{pk}" for pk in global_primary_keys]
        join_keys = array_pks + array_foreign_keys

        # Rename all columns in target_df with 'target_' prefix
        target_df = target_df.select(
            *[col(c).alias(f"target_{c}") for c in target_df.columns]
        )
        log(f"Target DataFrame columns for '{array_col}' prefixed with 'target_'.", level="DEBUG")

        # Build join conditions using source and target columns
        join_conditions = [
            source_df[pk] == target_df[f"target_{pk}"] for pk in join_keys
        ]
        log(f"Join conditions for '{array_col}': {join_keys}", level="DEBUG")

        # Perform the join
        joined_df = source_df.join(target_df, on=join_conditions, how="full_outer")
        joined_dfs[array_col] = joined_df
        log(f"Joined DataFrame for '{array_col}' created successfully.", level="INFO")

    return joined_dfs

def compare_and_combine_differences(joined_df, compare_fields, source_prefix, target_prefix, global_primary_keys, array_pks, result_col_name):
    """
    Compare fields between source and target DataFrames and combine differences into a single column.

    Parameters:
        joined_df (DataFrame): The joined DataFrame.
        compare_fields (list or None): List of field names to compare. If None or empty, compare all non-primary key fields.
        source_prefix (str): Prefix of source fields.
        target_prefix (str): Prefix of target fields.
        global_primary_keys (list): List of global primary key column names.
        array_pks (list): List of array-specific primary key column names.
        result_col_name (str): Name of the result column for combined differences.

    Returns:
        DataFrame: DataFrame with primary keys and combined differences.
    """
    log(f"Comparing fields: {compare_fields}", level="DEBUG")
    difference_expressions = []
    for field in compare_fields:
        source_field = f"{source_prefix}{field}"
        target_field = f"{target_prefix}{field}"

        log(f"Comparing field: {field}", level="DEBUG")
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
        log(f"Difference expression for '{field}' added.", level="DEBUG")

    # Combine differences into a single column
    combined_differences = concat_ws("; ", array(*difference_expressions))
    log("Combined differences column created.", level="DEBUG")

    # Select primary keys and combined differences
    selected_columns = [col(pk) for pk in global_primary_keys + array_pks] + \
                       [col(f"fk_{pk}") for pk in global_primary_keys]
    log("Selected columns for the result DataFrame.", level="DEBUG")

    result_df = joined_df.select(
        *selected_columns,
        combined_differences.alias(result_col_name)
    ).filter(col(result_col_name).isNotNull())

    log(f"Differences identified and combined in column '{result_col_name}'.", level="INFO")
    return result_df

# ------------------------------------
# 6. Data Reconciliation Process
# ------------------------------------

log("Starting DataFrame reconciliation process.", level="INFO")

# Define global and array-specific primary keys
global_primary_keys = ["parent_primary_key", "child_primary_key"]
array_columns = {
    "orders": ["order_id"],
    "payments": ["payment_id"],
    "new_array": ["new_id1", "new_id2"]
}

# Explode arrays and prefix columns for source
log("Exploding and prefixing arrays for source DataFrame.", level="INFO")
source_exploded_dfs = explode_and_prefix(source_df, array_columns, "source_", global_primary_keys)

# Explode arrays and prefix columns for target (no prefix for target fields to avoid double-prefixing)
log("Exploding and prefixing arrays for target DataFrame.", level="INFO")
target_exploded_dfs = explode_and_prefix(target_df, array_columns, "", global_primary_keys)

# Join exploded DataFrames
log("Joining exploded DataFrames.", level="INFO")
joined_dfs = join_exploded_dfs(source_exploded_dfs, target_exploded_dfs, array_columns, global_primary_keys)

# Define fields to compare for each array column
fields_to_compare = {
    "orders": ["order_date", "amount", "status"],
    "payments": ["payment_date", "method", "amount"],
    "new_array": ["detail"]
}

# Compare fields and collect differences
difference_results = {}
for array_col, array_pks in array_columns.items():
    log(f"Processing comparison for array column: {array_col}", level="INFO")
    joined_df = joined_dfs[array_col]
    compare_fields = fields_to_compare.get(array_col)
    result_col_name = f"{array_col}_differences"
    diff_df = compare_and_combine_differences(
        joined_df, compare_fields, "source_", "target_", global_primary_keys, array_pks, result_col_name
    )
    difference_results[array_col] = diff_df
    log(f"Differences for '{array_col}' collected successfully.", level="INFO")

# ------------------------------------
# 7. Display the Differences
# ------------------------------------

def display_differences(difference_results):
    for array_col, diff_df in difference_results.items():
        log(f"=== Differences in {array_col.capitalize()} ===", level="INFO")
        diff_df.show(truncate=False)
        log(f"=== End of Differences in {array_col.capitalize()} ===\n", level="INFO")

log("Displaying all identified differences.", level="INFO")
display_differences(difference_results)

# ------------------------------------
# 8. Stop the SparkSession
# ------------------------------------

log("DataFrame reconciliation process completed. Stopping SparkSession.", level="INFO")
spark.stop()
