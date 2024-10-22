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

# Schema definitions (same as before)
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
    StructField("detail", StringType(), True),
    StructField("extra_info", StringType(), True)          # Additional field for testing
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

# Sample data for source and target DataFrames (same as before)
# ... [Include source_data and target_data as previously defined]

# Create DataFrames with the defined schemas
source_df = spark.createDataFrame(source_data, main_schema)
target_df = spark.createDataFrame(target_data, main_schema)

def explode_and_prefix(df, array_columns, prefix, global_primary_keys):
    exploded_dfs = {}
    for array_col, array_pks in array_columns.items():
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
            elif field in foreign_keys:
                # Prefix foreign keys with 'fk_'
                selected_cols.append(col(full_field).alias(f"fk_{field}"))
            else:
                # Prefix other fields
                selected_cols.append(col(full_field).alias(f"{prefix}{field}"))

        # Add global primary keys from parent DataFrame
        for pk in global_primary_keys:
            selected_cols.append(col(pk))

        # Create the exploded DataFrame
        exploded_df = exploded_col.select(*selected_cols)
        exploded_dfs[array_col] = exploded_df

    return exploded_dfs

def join_exploded_dfs(source_dfs, target_dfs, array_columns, global_primary_keys):
    joined_dfs = {}
    for array_col, array_pks in array_columns.items():
        source_df = source_dfs[array_col]
        target_df = target_dfs[array_col]

        # Build join keys: array-specific primary keys and foreign keys
        array_foreign_keys = [f"fk_{pk}" for pk in global_primary_keys]
        join_keys = array_pks + array_foreign_keys

        # Rename all columns in target_df with 'target_' prefix
        target_df = target_df.select(
            *[col(c).alias(f"target_{c}") for c in target_df.columns]
        )

        # Build join conditions using source and target columns
        join_conditions = [
            source_df[pk] == target_df[f"target_{pk}"] for pk in join_keys
        ]

        # Perform the join
        joined_df = source_df.join(target_df, on=join_conditions, how="full_outer")

        joined_dfs[array_col] = joined_df

    return joined_dfs

def compare_and_combine_differences(
        joined_df, compare_fields, source_prefix, target_prefix,
        global_primary_keys, array_pks, result_col_name
):
    # If compare_fields is None or empty, compare all fields except primary and foreign keys
    if not compare_fields:
        # Get all columns from joined_df
        all_columns = joined_df.columns

        # Exclude primary keys and foreign keys
        exclude_fields = global_primary_keys + array_pks + [f"fk_{pk}" for pk in global_primary_keys]
        exclude_fields += [f"{source_prefix}{pk}" for pk in array_pks]
        exclude_fields += [f"{target_prefix}{pk}" for pk in array_pks]

        # Fields to compare
        compare_fields = []
        for col_name in all_columns:
            if col_name.startswith(source_prefix):
                field = col_name[len(source_prefix):]
                source_field = f"{source_prefix}{field}"
                target_field = f"{target_prefix}{field}"
                if (
                        source_field in all_columns and
                        target_field in all_columns and
                        field not in exclude_fields and
                        source_field not in exclude_fields and
                        target_field not in exclude_fields
                ):
                    compare_fields.append(field)

    difference_expressions = []
    for field in compare_fields:
        source_field = f"{source_prefix}{field}"
        target_field = f"{target_prefix}{field}"

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

    if difference_expressions:
        # Combine differences into a single column
        combined_differences = concat_ws("; ", array(*difference_expressions))

        # Select primary keys and combined differences
        selected_columns = [col(pk) for pk in global_primary_keys + array_pks] + \
                           [col(f"fk_{pk}") for pk in global_primary_keys]
        result_df = joined_df.select(
            *selected_columns,
            combined_differences.alias(result_col_name)
        ).filter(col(result_col_name).isNotNull())
    else:
        # If no differences found, return an empty DataFrame with the required columns
        result_df = spark.createDataFrame([], joined_df.schema)

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
# For target_df, use an empty prefix to avoid double-prefixing
target_exploded_dfs = explode_and_prefix(target_df, array_columns, "", global_primary_keys)

# Join exploded DataFrames
joined_dfs = join_exploded_dfs(source_exploded_dfs, target_exploded_dfs, array_columns, global_primary_keys)

# Define fields to compare for each array column
# If no fields are specified, set compare_fields to None or an empty list
fields_to_compare = {
    "orders": [],        # Empty list means compare all fields
    "payments": None,    # None means compare all fields
    "new_array": ["detail"]  # Compare only 'detail' field
}

# Compare fields and collect differences
difference_results = {}
for array_col, array_pks in array_columns.items():
    joined_df = joined_dfs[array_col]
    compare_fields = fields_to_compare.get(array_col)
    result_col_name = f"{array_col}_differences"
    diff_df = compare_and_combine_differences(
        joined_df, compare_fields, "source_", "target_", global_primary_keys, array_pks, result_col_name
    )
    difference_results[array_col] = diff_df

# Display the differences
def display_differences(difference_results):
    for array_col, diff_df in difference_results.items():
        print(f"=== Differences in {array_col.capitalize()} ===")
        diff_df.show(truncate=False)
        print("\n")

display_differences(difference_results)

# Stop the SparkSession
spark.stop()
