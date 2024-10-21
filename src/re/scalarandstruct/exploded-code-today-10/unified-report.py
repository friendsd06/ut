from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, ArrayType, DoubleType
)
from pyspark.sql.functions import (
    explode_outer, col, when, lit, concat, coalesce, array, concat_ws
)
from functools import reduce

# Initialize SparkSession
spark = SparkSession.builder.appName("UnifiedDifferencesReport").getOrCreate()

# Define schemas for nested structures
address_schema = StructType([
    StructField("street", StringType(), True),
    StructField("city", StringType(), True),
    StructField("zipcode", StringType(), True)
])

order_schema = StructType([
    StructField("order_id", StringType(), True),
    StructField("parent_primary_key", StringType(), True),
    StructField("child_primary_key", StringType(), True),
    StructField("order_date", StringType(), True),
    StructField("amount", DoubleType(), True),
    StructField("status", StringType(), True)
])

payment_schema = StructType([
    StructField("payment_id", StringType(), True),
    StructField("parent_primary_key", StringType(), True),
    StructField("child_primary_key", StringType(), True),
    StructField("payment_date", StringType(), True),
    StructField("method", StringType(), True),
    StructField("amount", DoubleType(), True)
])

new_array_schema = StructType([
    StructField("new_id1", StringType(), True),
    StructField("new_id2", StringType(), True),
    StructField("parent_primary_key", StringType(), True),
    StructField("child_primary_key", StringType(), True),
    StructField("detail", StringType(), True),
    StructField("extra_info", StringType(), True)
])

# Main schema
main_schema = StructType([
    StructField("parent_primary_key", StringType(), True),
    StructField("child_primary_key", StringType(), True),
    StructField("name", StringType(), True),
    StructField("age", IntegerType(), True),
    StructField("address", address_schema, True),
    StructField("orders", ArrayType(order_schema), True),
    StructField("payments", ArrayType(payment_schema), True),
    StructField("new_array", ArrayType(new_array_schema), True)
])

# Sample data for source and target DataFrames
source_data = [
    {
        "parent_primary_key": "P1",
        "child_primary_key": "C1",
        "name": "John Doe",
        "age": 30,
        "address": {"street": "123 Main St", "city": "New York", "zipcode": "10001"},
        "orders": [
            {"order_id": "O1001", "parent_primary_key": "P1", "child_primary_key": "C1",
             "order_date": "2023-01-10", "amount": 250.0, "status": "Shipped"},
            {"order_id": "O1002", "parent_primary_key": "P1", "child_primary_key": "C1",
             "order_date": "2023-02-15", "amount": 150.0, "status": "Processing"},
            {"order_id": "O1004", "parent_primary_key": "P1", "child_primary_key": "C1",
             "order_date": "2023-04-01", "amount": 200.0, "status": "Pending"}
        ],
        "payments": [
            {"payment_id": "PM2001", "parent_primary_key": "P1", "child_primary_key": "C1",
             "payment_date": "2023-02-16", "method": "Credit Card", "amount": 150.0}
        ],
        "new_array": [
            {"new_id1": "N3001", "new_id2": "N4001", "parent_primary_key": "P1", "child_primary_key": "C1",
             "detail": "Detail A", "extra_info": "Extra A"},
            {"new_id1": "N3002", "new_id2": "N4002", "parent_primary_key": "P1", "child_primary_key": "C1",
             "detail": "Detail B", "extra_info": "Extra B"}
        ]
    },
    {
        "parent_primary_key": "P2",
        "child_primary_key": "C2",
        "name": "Jane Smith",
        "age": 28,
        "address": {"street": "456 Elm St", "city": "Los Angeles", "zipcode": "90001"},
        "payments": [
            {"payment_id": "PM2002", "parent_primary_key": "P2", "child_primary_key": "C2",
             "payment_date": "2023-03-21", "method": "PayPal", "amount": 300.0}
        ],
        "new_array": [
            {"new_id1": "N3003", "new_id2": "N4003", "parent_primary_key": "P2", "child_primary_key": "C2",
             "detail": "Detail C", "extra_info": "Extra C"}
        ]
    }
]

target_data = [
    {
        "parent_primary_key": "P1",
        "child_primary_key": "C1",
        "name": "John Doe",
        "age": 31,  # Age updated
        "address": {"street": "123 Main St", "city": "New York", "zipcode": "10001"},
        "orders": [
            {"order_id": "O1001", "parent_primary_key": "P1", "child_primary_key": "C1",
             "order_date": "2023-01-10", "amount": 250.0, "status": "Shipped"},
            {"order_id": "O1002", "parent_primary_key": "P1", "child_primary_key": "C1",
             "order_date": "2023-02-15", "amount": 175.0, "status": "Completed"},  # amount and status updated
            {"order_id": "O1005", "parent_primary_key": "P1", "child_primary_key": "C1",
             "order_date": "2023-05-01", "amount": 300.0, "status": "Processing"}  # New order
        ],
        "payments": [
            {"payment_id": "PM2001", "parent_primary_key": "P1", "child_primary_key": "C1",
             "payment_date": "2023-02-16", "method": "Credit Card", "amount": 150.0}
        ],
        "new_array": [
            {"new_id1": "N3001", "new_id2": "N4001", "parent_primary_key": "P1", "child_primary_key": "C1",
             "detail": "Detail A", "extra_info": "Extra A"},
            {"new_id1": "N3002", "new_id2": "N4002", "parent_primary_key": "P1", "child_primary_key": "C1",
             "detail": "Detail B Modified", "extra_info": "Extra B"}  # detail updated
        ]
    },
    {
        "parent_primary_key": "P2",
        "child_primary_key": "C2",
        "name": "Jane Smith",
        "age": 28,
        "address": {"street": "789 Pine St", "city": "San Francisco", "zipcode": "94101"},  # Address updated
        "payments": [
            {"payment_id": "PM2002", "parent_primary_key": "P2", "child_primary_key": "C2",
             "payment_date": "2023-03-21", "method": "Credit Card", "amount": 300.0}  # method updated
        ],
        "new_array": [
            {"new_id1": "N3003", "new_id2": "N4003", "parent_primary_key": "P2", "child_primary_key": "C2",
             "detail": "Detail C", "extra_info": "Extra C"},
            {"new_id1": "N3004", "new_id2": "N4004", "parent_primary_key": "P2", "child_primary_key": "C2",
             "detail": "Detail D", "extra_info": "Extra D"}  # New entry
        ]
    }
]

# Create DataFrames with the defined schemas
source_df = spark.createDataFrame(source_data, main_schema)
target_df = spark.createDataFrame(target_data, main_schema)

def explode_and_prefix(df, array_columns, prefix, global_primary_keys):
    """Explode array columns and prefix field names."""
    exploded_dfs = {}
    for array_col, array_pks in array_columns.items():
        # Explode the array column
        exploded_df = df.withColumn(f"{array_col}_exploded", explode_outer(col(array_col)))
        # Flatten the exploded struct
        struct_fields = exploded_df.select(f"{array_col}_exploded.*").columns

        # Identify foreign keys
        foreign_keys = set(global_primary_keys) & set(struct_fields)

        # Select and prefix columns
        selected_cols = []
        for field in struct_fields:
            full_field = f"{array_col}_exploded.{field}"
            if field in array_pks:
                # Keep array-specific primary keys unprefixed
                selected_cols.append(col(full_field))
            elif field in foreign_keys:
                # Prefix foreign keys with 'fk_'
                selected_cols.append(col(full_field).alias(f"fk_{field}"))
            else:
                # Prefix other fields
                selected_cols.append(col(full_field).alias(f"{prefix}{field}"))

        # Add global primary keys
        for pk in global_primary_keys:
            selected_cols.append(col(pk))

        # Create the exploded DataFrame
        exploded_dfs[array_col] = exploded_df.select(*selected_cols)
    return exploded_dfs

def join_exploded_dfs(source_dfs, target_dfs, array_columns, global_primary_keys):
    """Join exploded DataFrames from source and target."""
    joined_dfs = {}
    for array_col, array_pks in array_columns.items():
        source_df = source_dfs[array_col]
        target_df = target_dfs[array_col]

        # Build join keys
        array_foreign_keys = [f"fk_{pk}" for pk in global_primary_keys]
        join_keys = array_pks + array_foreign_keys

        # Prefix target columns
        target_df = target_df.select(
            *[col(c).alias(f"target_{c}") for c in target_df.columns]
        )

        # Build join conditions
        join_conditions = [
            source_df[pk] == target_df[f"target_{pk}"] for pk in join_keys
        ]

        # Perform the join
        joined_df = source_df.join(target_df, on=join_conditions, how="full_outer")
        joined_dfs[array_col] = joined_df
    return joined_dfs

def compare_and_collect_differences(
        joined_df, compare_fields, source_prefix, target_prefix,
        global_primary_keys, array_pks, result_col_name
):
    """Compare fields and collect differences."""
    if not compare_fields:
        # Determine fields to compare
        all_columns = joined_df.columns
        exclude_fields = global_primary_keys + array_pks + [f"fk_{pk}" for pk in global_primary_keys]
        exclude_fields += [f"{source_prefix}{pk}" for pk in array_pks]
        exclude_fields += [f"{target_prefix}{pk}" for pk in array_pks]

        compare_fields = []
        for col_name in all_columns:
            if col_name.startswith(source_prefix):
                field = col_name[len(source_prefix):]
                source_field = f"{source_prefix}{field}"
                target_field = f"{target_prefix}{field}"
                if (
                        source_field in all_columns and
                        target_field in all_columns and
                        field not in exclude_fields
                ):
                    compare_fields.append(field)

    # Initialize difference column
    joined_df = joined_df.withColumn(result_col_name, lit(None))

    # Compare each field and build the differences
    for field in compare_fields:
        source_field = f"{source_prefix}{field}"
        target_field = f"{target_prefix}{field}"

        if source_field in joined_df.columns and target_field in joined_df.columns:
            source_dtype = dict(joined_df.dtypes).get(source_field)
            target_dtype = dict(joined_df.dtypes).get(target_field)

            if source_dtype == target_dtype:
                difference = when(
                    col(source_field).isNull() & col(target_field).isNotNull(),
                    concat(lit(f"{field}: Source = null, Target = "), col(target_field).cast("string"))
                ).when(
                    col(source_field).isNotNull() & col(target_field).isNull(),
                    concat(lit(f"{field}: Source = "), col(source_field).cast("string"), lit(", Target = null"))
                ).when(
                    col(source_field) != col(target_field),
                    concat(
                        lit(f"{field}: Source = "), col(source_field).cast("string"),
                        lit(", Target = "), col(target_field).cast("string")
                    )
                )
                # Update the differences column
                joined_df = joined_df.withColumn(
                    result_col_name,
                    when(difference.isNotNull(),
                         when(col(result_col_name).isNull(), difference)
                         .otherwise(concat_ws("; ", col(result_col_name), difference))
                         ).otherwise(col(result_col_name))
                )

    # Filter rows with differences
    result_df = joined_df.filter(col(result_col_name).isNotNull())

    # Select relevant columns
    selected_columns = global_primary_keys + array_pks + [result_col_name]
    result_df = result_df.select(*selected_columns)

    return result_df

# Define global and array-specific primary keys
global_primary_keys = ["parent_primary_key", "child_primary_key"]
array_columns = {
    "orders": ["order_id"],
    "payments": ["payment_id"],
    "new_array": ["new_id1", "new_id2"]
}

# Explode arrays and prefix columns
source_exploded_dfs = explode_and_prefix(source_df, array_columns, "source_", global_primary_keys)
target_exploded_dfs = explode_and_prefix(target_df, array_columns, "", global_primary_keys)  # No prefix for target

# Join exploded DataFrames
joined_dfs = join_exploded_dfs(source_exploded_dfs, target_exploded_dfs, array_columns, global_primary_keys)

# Define fields to compare for each array
fields_to_compare = {
    "orders": [],        # Compare all fields
    "payments": None,    # Compare all fields
    "new_array": ["detail"]  # Compare only 'detail' field
}

# Collect differences for each array
difference_results = {}
for array_col, array_pks in array_columns.items():
    joined_df = joined_dfs[array_col]
    compare_fields = fields_to_compare.get(array_col)
    result_col_name = f"{array_col}_differences"
    diff_df = compare_and_collect_differences(
        joined_df, compare_fields, "source_", "target_",
        global_primary_keys, array_pks, result_col_name
    )
    difference_results[array_col] = diff_df

# Start with keys from both source and target DataFrames
all_keys_df = source_df.select(global_primary_keys).union(
    target_df.select(global_primary_keys)
).distinct()

# Function to merge differences DataFrames
def merge_differences(df1, df2):
    return df1.join(df2, on=global_primary_keys, how='full_outer')

# Initialize unified differences DataFrame
unified_diff_df = all_keys_df

# Merge differences for each array
for array_col, diff_df in difference_results.items():
    # Select necessary columns
    diff_columns = global_primary_keys + [col for col in diff_df.columns if col.endswith('_differences')]
    diff_df = diff_df.select(diff_columns)
    # Merge with unified DataFrame
    unified_diff_df = merge_differences(unified_diff_df, diff_df)

# Display the unified differences report
print("=== Unified Differences Report ===")
unified_diff_df.show(truncate=False)

# Stop the SparkSession
spark.stop()
