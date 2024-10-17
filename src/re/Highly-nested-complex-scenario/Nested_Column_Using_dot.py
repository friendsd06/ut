from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col, when, lit, struct, array, explode, array_remove, size, concat_ws, concat, collect_list
)
from typing import List, Optional

def compare_dataframes_with_nested_columns(
        source_df: DataFrame,
        target_df: DataFrame,
        join_key: str,
        columns_to_compare: List[str],
        include_columns: Optional[List[str]] = None,
        exclude_columns: Optional[List[str]] = None,
        parent_prefix: str = "PARENT_"
) -> DataFrame:
    """
    Compare two DataFrames and return a DataFrame with mismatches, including nested columns.

    Args:
        source_df (DataFrame): Source DataFrame.
        target_df (DataFrame): Target DataFrame.
        join_key (str): Column to join on.
        columns_to_compare (List[str]): List of columns to compare, including nested columns using dot notation.
        include_columns (Optional[List[str]]): Specific columns to include in the comparison.
        exclude_columns (Optional[List[str]]): Specific columns to exclude from the comparison.
        parent_prefix (str): Prefix to add to parent attributes in mismatch description.

    Returns:
        DataFrame: DataFrame containing join_key and concatenated differences where source != target
    """
    # Apply include and exclude filters
    if include_columns:
        compare_columns = list(set(include_columns) & set(columns_to_compare))
    else:
        compare_columns = list(set(columns_to_compare))

    if exclude_columns:
        compare_columns = [c for c in compare_columns if c not in exclude_columns]

    if not compare_columns:
        raise ValueError("No columns available for comparison after applying include/exclude filters.")

    # Perform a full outer join on join_key
    joined_df = source_df.alias("source").join(target_df.alias("target"), on=join_key, how="full_outer")

    # Create mismatch expressions
    mismatch_structs = []
    for c in compare_columns:
        # Handle nested columns using dot notation
        parts = c.split('.')
        if len(parts) > 1:
            parent = '.'.join(parts[:-1])
            child = parts[-1]
            column_name = f"{parent}.{child}"
            source_col = f"source.{c}"
            target_col = f"target.{c}"
        else:
            parent = "root"
            child = c
            column_name = c
            source_col = f"source.{c}"
            target_col = f"target.{c}"

        # Define the mismatch condition
        mismatch = when(
            (col(source_col).cast("string") != col(target_col).cast("string")) |
            (col(source_col).isNull() != col(target_col).isNull()),
            struct(
                lit(column_name).alias("column"),
                lit(parent).alias("parent"),
                col(source_col).alias("source_value"),
                col(target_col).alias("target_value")
            )
        ).otherwise(None)

        mismatch_structs.append(mismatch)

    # Aggregate mismatches into an array
    mismatches_array = array(*mismatch_structs)

    # Remove null entries from the array
    mismatches_clean = array_remove(mismatches_array, None)

    # Add mismatches array column
    comparison_df = joined_df.withColumn("mismatches", mismatches_clean)

    # Calculate mismatch_count and match_count
    comparison_df = comparison_df.withColumn(
        "mismatch_count",
        size("mismatches")
    ).withColumn(
        "match_count",
        lit(len(compare_columns)) - col("mismatch_count")
    )

    # Explode mismatches to have one row per mismatch
    exploded_df = comparison_df.withColumn("mismatch", explode("mismatches")) \
        .select(
        col(join_key),
        (lit(parent_prefix) + col("mismatch.parent") + "_" + col("mismatch.column")).alias("column"),
        col("mismatch.source_value").alias("source_value"),
        col("mismatch.target_value").alias("target_value"),
        col("mismatch_count"),
        col("match_count")
    )

    # Aggregate differences into a concatenated string per join_key
    differences_df = exploded_df.groupBy(join_key, "mismatch_count", "match_count") \
        .agg(
        concat_ws(", ",
                  collect_list(
                      concat_ws(": source=",
                                col("column"),
                                concat_ws(", target=", col("source_value"), col("target_value"))
                                )
                  )
                  ).alias("differences")
    ).filter(col("differences").isNotNull())

    return differences_df


    # Define schema for source DataFrame
    schema_source = StructType([
        StructField("id", IntegerType(), True),
        StructField("col_a", StringType(), True),
        StructField("nested_col", StructType([
            StructField("id", IntegerType(), True),
            StructField("name", StringType(), True)
        ]), True)
    ])

    # Define schema for target DataFrame
    schema_target = StructType([
        StructField("id", IntegerType(), True),
        StructField("col_a", StringType(), True),
        StructField("nested_col", StructType([
            StructField("id", IntegerType(), True),
            StructField("name", StringType(), True)
        ]), True)
    ])

    # Create sample data for source DataFrame
    data_source = [
        (1, "A1", {"id": 100, "name": "John"}),
        (2, "A2", {"id": 101, "name": "Jane"}),
        (3, "A3", {"id": 102, "name": "Alice"})
    ]

    # Create sample data for target DataFrame
    data_target = [
        (1, "A1", {"id": 100, "name": "John"}),  # No mismatch
        (2, "A2_modified", {"id": 101, "name": "Jane Doe"}),  # col_a and nested_col.name mismatch
        (4, "A4", {"id": 103, "name": "Bob"})  # New record not in source
    ]

    # Create DataFrames
    source_df = spark.createDataFrame(data_source, schema_source)
    target_df = spark.createDataFrame(data_target, schema_target)

    # Define join key
    join_key = "id"

    # Define columns to compare
    columns_to_compare = [
        "col_a",
        "nested_col.id",
        "nested_col.name"
    ]

    # Perform reconciliation
    differences_df = compare_dataframes_with_nested_columns(
        source_df=source_df,
        target_df=target_df,
        join_key=join_key,
        columns_to_compare=columns_to_compare,
        include_columns=columns_to_compare,
        exclude_columns=[]
    )

    # Show differences
    print("=== Example 1: Simple Flat DataFrames ===")
    differences_df.show(truncate=False)