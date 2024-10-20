from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, ArrayType, DoubleType
)
from pyspark.sql.functions import (
    explode_outer, col, when, lit, concat, coalesce, array, concat_ws
)
from functools import reduce

# Initialize SparkSession
spark = SparkSession.builder.appName("Comparison").getOrCreate()

# Schema for 'address' struct
address_schema = StructType([
    StructField("street", StringType(), True),
    StructField("city", StringType(), True),
    StructField("zipcode", StringType(), True)
])

# Schema for 'order' struct
order_schema = StructType([
    StructField("order_id", StringType(), True),
    StructField("order_date", StringType(), True),
    StructField("amount", DoubleType(), True),
    StructField("status", StringType(), True)
])

# Schema for 'payment' struct
payment_schema = StructType([
    StructField("payment_id", StringType(), True),
    StructField("payment_date", StringType(), True),
    StructField("method", StringType(), True),
    StructField("amount", DoubleType(), True)
])

# Schema for 'new_array' struct with multiple primary keys
new_array_schema = StructType([
    StructField("new_id1", StringType(), True),
    StructField("new_id2", StringType(), True),
    StructField("detail", StringType(), True)
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
    StructField("new_array", ArrayType(new_array_schema), True)  # Added new_array
])

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
            {"order_id": "O1002", "order_date": "2023-02-15", "amount": 150.0, "status": "Processing"},
            {"order_id": "O1004", "order_date": "2023-04-01", "amount": 200.0, "status": "Pending"}  # Extra order
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
            {"order_id": "O1002", "order_date": "2023-02-15", "amount": 175.0, "status": "Completed"},  # Differences here
            {"order_id": "O1005", "order_date": "2023-05-01", "amount": 300.0, "status": "Processing"}  # New order
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

# Functions remain the same, except for the fix
def explode_and_prefix(df, array_columns, prefix, global_primary_keys):
    exploded_dfs = {}
    for array_col, array_pks in array_columns.items():
        # Explode the array column
        exploded_col = df.withColumn(f"{array_col}_exploded", explode_outer(col(array_col)))

        # Select global primary keys
        selected_cols = [col(pk) for pk in global_primary_keys]

        # Select array-specific fields and prefix them
        struct_fields = exploded_col.select(f"{array_col}_exploded.*").columns
        prefixed_fields = [col(f"{array_col}_exploded.{field}").alias(f"{prefix}{field}") for field in struct_fields]
        selected_cols += prefixed_fields

        # Select the relevant columns
        exploded_df = exploded_col.select(*selected_cols)
        exploded_dfs[array_col] = exploded_df

    return exploded_dfs

def join_exploded_dfs(source_dfs, target_dfs, array_columns, global_primary_keys):
    joined_dfs = {}
    for array_col, array_pks in array_columns.items():
        source_df = source_dfs[array_col]
        target_df = target_dfs[array_col]

        # Rename target's array-specific primary keys with 'target_' prefix
        for pk in array_pks:
            target_df = target_df.withColumnRenamed(pk, f"target_{pk}")

        # Build join conditions: global primary keys and array-specific primary keys
        join_conditions = []
        for pk in global_primary_keys:
            join_conditions.append(source_df[pk] == target_df[pk])
        for pk in array_pks:
            source_pk = f"{pk}"
            target_pk = f"target_{pk}"
            join_conditions.append(source_df[source_pk] == target_df[target_pk])

        # Combine all join conditions with AND
        if join_conditions:
            join_condition = reduce(lambda x, y: x & y, join_conditions)
        else:
            join_condition = lit(True)  # If no conditions, perform cross join

        # Perform the join
        joined_df = source_df.join(target_df, on=join_condition, how="full_outer")

        joined_dfs[array_col] = joined_df

    return joined_dfs

def compare_and_combine_differences(joined_df, compare_fields, source_prefix, target_prefix, global_primary_keys, array_pks, result_col_name):
    # Difference expressions
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

    # Select global primary keys and array-specific primary keys
    selected_columns = [
                           col(pk) for pk in global_primary_keys
                       ] + [
                           col(pk) for pk in array_pks
                       ] + [
                           col(f"target_{pk}") for pk in array_pks
                       ]

    # Select primary keys and combined differences
    result_df = joined_df.select(
        *selected_columns,
        combined_differences.alias(result_col_name)
    ).filter(col(result_col_name).isNotNull())

    return result_df

# Define global primary keys
global_primary_keys = ["parent_primary_key", "child_primary_key"]

# Define array columns to process with their respective array-specific primary keys
array_columns = {
    "orders": ["order_id"],
    "payments": ["payment_id"],
    "new_array": ["new_id1", "new_id2"]  # Example of an array with multiple primary keys
}

# Explode and prefix arrays in source and target DataFrames
source_exploded_dfs = explode_and_prefix(source_df, array_columns, "source_", global_primary_keys)
target_exploded_dfs = explode_and_prefix(target_df, array_columns, "target_", global_primary_keys)

# Join exploded DataFrames
joined_dfs = join_exploded_dfs(source_exploded_dfs, target_exploded_dfs, array_columns, global_primary_keys)

# Define fields to compare for each array column, including primary keys
fields_to_compare = {
    "orders": ["order_id", "order_date", "amount", "status"],
    "payments": ["payment_id", "payment_date", "method", "amount"],
    "new_array": ["new_id1", "new_id2", "detail"]
}

# Compare fields and combine differences
difference_results = {}
for array_col, array_pks in array_columns.items():
    if array_col not in joined_dfs:
        continue  # Skip if no joined DataFrame for the array column
    joined_df = joined_dfs[array_col]
    compare_fields = fields_to_compare.get(array_col)  # Will be None if not specified
    result_col_name = f"{array_col}_differences"
    diff_df = compare_and_combine_differences(
        joined_df,
        compare_fields,
        "source_",
        "target_",
        global_primary_keys,
        array_pks,
        result_col_name
    )
    difference_results[array_col] = diff_df

# Function to display differences
def display_differences(difference_results, array_columns):
    for array_col in array_columns.keys():
        print(f"=== Differences in {array_col.capitalize()} ===")
        if array_col in difference_results:
            difference_results[array_col].show(truncate=False)
        else:
            print("No differences found.")
        print("\n")

# Display the differences
display_differences(difference_results, array_columns)

# Stop the SparkSession
spark.stop()
