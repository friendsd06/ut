from pyspark.sql import SparkSession
from pyspark.sql.functions import explode_outer, col, when, concat, lit, coalesce, array, concat_ws
from functools import reduce
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, ArrayType

# Initialize SparkSession
spark = SparkSession.builder.appName("NestedKeyExample").getOrCreate()

# Sample Data
source_data = [
    {
        "parent_primary_key": "P1",
        "customer_name": "Alice",
        "orders": [
            {"order_line_id": "OL1", "product": "Widget", "quantity": 5},
            {"order_line_id": "OL2", "product": "Gadget", "quantity": 3}
        ]
    },
    {
        "parent_primary_key": "P2",
        "customer_name": "Bob",
        "orders": [
            {"order_line_id": "OL3", "product": "Thingamajig", "quantity": 2}
        ]
    }
]

target_data = [
    {
        "parent_primary_key": "P1",
        "customer_name": "Alice",
        "orders": [
            {"order_line_id": "OL1", "product": "Widget", "quantity": 4},  # Quantity difference
            {"order_line_id": "OL2", "product": "Gadget", "quantity": 3}
        ]
    },
    {
        "parent_primary_key": "P2",
        "customer_name": "Bob",
        "orders": [
            {"order_line_id": "OL3", "product": "Thingamajig", "quantity": 2}
        ]
    }
]

# Define schema
order_schema = StructType([
    StructField("order_line_id", StringType()),
    StructField("product", StringType()),
    StructField("quantity", IntegerType())
])

schema = StructType([
    StructField("parent_primary_key", StringType()),
    StructField("customer_name", StringType()),
    StructField("orders", ArrayType(order_schema))
])

# Create DataFrames
source_df = spark.createDataFrame(source_data, schema)
target_df = spark.createDataFrame(target_data, schema)

# Functions as defined above
def explode_and_prefix(df, array_cols, prefix, parent_keys, nested_keys):
    """
    Explode specified array columns and prefix the resulting fields.

    Parameters:
        df (DataFrame): The input DataFrame.
        array_cols (list): List of array column names to explode.
        prefix (str): Prefix to add to the exploded fields.
        parent_keys (list): List of primary key column names at the parent level.
        nested_keys (dict): Dictionary mapping array column names to their nested key names.

    Returns:
        dict: Dictionary of DataFrames with exploded and prefixed fields.
    """
    exploded_dfs = {}
    for col_name in array_cols:
        # Explode the array column
        exploded_col = df.withColumn(f"{col_name}_exploded", explode_outer(col(col_name)))
        # Select parent keys, nested keys, and exploded struct fields
        struct_fields = exploded_col.select(f"{col_name}_exploded.*").columns
        prefixed_fields = []
        for field in struct_fields:
            if field == nested_keys.get(col_name):
                # Keep nested key without prefix
                prefixed_fields.append(col(f"{col_name}_exploded.{field}").alias(field))
            else:
                # Prefix other fields
                prefixed_fields.append(col(f"{col_name}_exploded.{field}").alias(f"{prefix}{field}"))
        selected_cols = parent_keys + prefixed_fields
        exploded_df = exploded_col.select(*selected_cols)
        exploded_dfs[col_name] = exploded_df
    return exploded_dfs


def join_exploded_dfs(source_dfs, target_dfs, array_cols, parent_keys, nested_keys):
    """
    Join source and target exploded DataFrames on parent keys and nested keys.

    Parameters:
        source_dfs (dict): Dictionary of source DataFrames keyed by array column names.
        target_dfs (dict): Dictionary of target DataFrames keyed by array column names.
        array_cols (list): List of array column names.
        parent_keys (list): List of parent-level primary key column names.
        nested_keys (dict): Dictionary mapping array column names to their nested key names.

    Returns:
        dict: Dictionary of joined DataFrames.
    """
    joined_dfs = {}
    for col_name in array_cols:
        source_df = source_dfs[col_name]
        target_df = target_dfs[col_name]

        # Rename keys in target_df to avoid ambiguity
        for key in parent_keys + [nested_keys[col_name]]:
            target_df = target_df.withColumnRenamed(key, f"{key}_target")

        # Build join condition
        source_join_keys = parent_keys + [nested_keys[col_name]]
        target_join_keys = [f"{key}_target" for key in source_join_keys]

        # Build join condition
        join_condition = reduce(lambda x, y: x & y, [
            col(s_key).eqNullSafe(col(t_key))
            for s_key, t_key in zip(source_join_keys, target_join_keys)
        ])

        # Perform the join
        joined_df = source_df.join(target_df, on=join_condition, how="full_outer")
        joined_dfs[col_name] = joined_df
    return joined_dfs


# Compare fields and combine differences
def compare_and_combine_differences(joined_df, compare_fields, source_prefix, target_prefix, parent_keys, nested_key, result_col_name):
    """
    Compare fields between source and target DataFrames and combine differences into a single column.

    Parameters:
        joined_df (DataFrame): The joined DataFrame.
        compare_fields (list): List of field names to compare.
        source_prefix (str): Prefix of source fields.
        target_prefix (str): Prefix of target fields.
        parent_keys (list): List of parent key column names.
        nested_key (str): Nested key column name.
        result_col_name (str): Name of the result column for combined differences.

    Returns:
        DataFrame: DataFrame with keys and combined differences.
    """
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

    combined_differences = concat_ws("; ", array(*difference_expressions))

    # Select keys and combined differences
    result_df = joined_df.select(
        *[col(key) if key in joined_df.columns else col(f"{key}_target").alias(key) for key in parent_keys],
        col(nested_key) if nested_key in joined_df.columns else col(f"{nested_key}_target").alias(nested_key),
        combined_differences.alias(result_col_name)
    ).filter(col(result_col_name).isNotNull())

    return result_df

difference_results = {}
for col_name in array_columns:
    joined_df = joined_dfs[col_name]
    compare_fields = fields_to_compare.get(col_name)
    nested_key = nested_keys[col_name]
    result_col_name = f"{col_name}_differences"
    diff_df = compare_and_combine_differences(
        joined_df,
        compare_fields,
        "source_",
        "target_",
        parent_keys,
        nested_key,
        result_col_name
    )
    difference_results[col_name] = diff_df

# Define primary keys
parent_keys = ["parent_primary_key"]
nested_keys = {"orders": "order_line_id"}
array_columns = ["orders"]

# Explode and prefix arrays in source and target DataFrames
source_exploded_dfs = explode_and_prefix(source_df, array_columns, "source_", parent_keys, nested_keys)
target_exploded_dfs = explode_and_prefix(target_df, array_columns, "target_", parent_keys, nested_keys)

# Join exploded DataFrames
joined_dfs = join_exploded_dfs(source_exploded_dfs, target_exploded_dfs, array_columns, parent_keys, nested_keys)

# Define fields to compare
fields_to_compare = {
    "orders": ["product", "quantity"]
}

# Compare fields and combine differences
difference_results = {}
for col_name in array_columns:
    joined_df = joined_dfs[col_name]
    compare_fields = fields_to_compare.get(col_name)
    nested_key = nested_keys[col_name]
    result_col_name = f"{col_name}_differences"
    diff_df = compare_and_combine_differences(
        joined_df,
        compare_fields,
        "source_",
        "target_",
        parent_keys,
        nested_key,
        result_col_name
    )
    difference_results[col_name] = diff_df

# Display Results
for col_name in array_columns:
    print(f"=== Differences in {col_name.capitalize()} ===")
    difference_results[col_name].show(truncate=False)

# Stop the SparkSession
spark.stop()
