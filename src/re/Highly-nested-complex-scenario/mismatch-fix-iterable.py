from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, ArrayType,
    MapType, DoubleType
)
from pyspark.sql.functions import (
    explode_outer, col, when, struct, lit, array, explode, array_remove, expr
)
from functools import reduce
from typing import List, Optional

def compare_delta_tables(
        table1: DataFrame,
        table2: DataFrame,
        join_key: str,
        include_columns: Optional[List[str]] = None,
        exclude_columns: Optional[List[str]] = None
) -> DataFrame:
    """
    Compare two Delta tables and return a DataFrame with the differences.
    Shows only the columns that have mismatches along with mismatch and match counts.
    Works for both nested and non-nested structures.

    Args:
        table1 (DataFrame): The first Delta table.
        table2 (DataFrame): The second Delta table.
        join_key (str): The primary key column to join on.
        include_columns (Optional[List[str]]): Specific columns to include in the comparison.
        exclude_columns (Optional[List[str]]): Specific columns to exclude from the comparison.

    Returns:
        DataFrame: A DataFrame highlighting the differences with mismatched columns,
                   their old and new values, and counts of mismatches and matches per row.
    """
    # Ensure the inputs are DataFrames
    if not isinstance(table1, DataFrame) or not isinstance(table2, DataFrame):
        raise ValueError("Both table1 and table2 must be Spark DataFrames.")

    # Determine columns for comparison
    all_columns = set(table1.columns) & set(table2.columns) - {join_key}

    # Include columns logic
    if include_columns:
        compare_columns = list(set(include_columns) & all_columns)
    else:
        compare_columns = list(all_columns)

    # Exclude columns logic
    if exclude_columns:
        compare_columns = [c for c in compare_columns if c not in exclude_columns]

    # Calculate total number of columns being compared
    total_compare_columns = len(compare_columns)

    if total_compare_columns == 0:
        raise ValueError("No columns available for comparison after applying include/exclude filters.")

    # Perform a full outer join
    joined_df = table1.alias("t1").join(table2.alias("t2"), on=join_key, how="full_outer")

    # Create comparison expressions
    # For each column, create a struct of (old, new) if values differ, else null
    compare_exprs = [
        when(col(f"t1.{c}") != col(f"t2.{c}"),
             struct(
                 col(f"t1.{c}").alias("old"),
                 col(f"t2.{c}").alias("new")
             )
             ).alias(c)
        for c in compare_columns
    ]

    # Select join_key and comparison expressions
    comparison_df = joined_df.select(
        col(f"t1.{join_key}").alias(join_key),
        *compare_exprs
    )

    # Create an array of mismatches with column name, old value, and new value
    mismatches_array = array(
        *[
            when(col(c).isNotNull(), struct(
                lit(c).alias("column"),
                col(f"{c}.old").alias("old_value"),
                col(f"{c}.new").alias("new_value")
            ))
            for c in compare_columns
        ]
    )

    # Remove nulls from the array
    mismatches_expr = array_remove(mismatches_array, None)

    # Add mismatches array column
    comparison_df = comparison_df.withColumn("mismatches", mismatches_expr)

    # Calculate mismatch_count and match_count
    comparison_df = comparison_df.withColumn(
        "mismatch_count",
        expr("size(mismatches)")
    ).withColumn(
        "match_count",
        lit(total_compare_columns) - col("mismatch_count")
    )

    # Explode mismatches to have one row per mismatch
    # Include mismatch_count and match_count in each row
    result_df = comparison_df.withColumn("mismatch", explode("mismatches")) \
        .select(
        join_key,
        col("mismatch.column").alias("column"),
        col("mismatch.old_value").alias("old_value"),
        col("mismatch.new_value").alias("new_value"),
        col("mismatch_count"),
        col("match_count")
    )

    return result_df
