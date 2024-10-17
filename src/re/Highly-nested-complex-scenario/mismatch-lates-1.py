from pyspark.sql import DataFrame
from pyspark.sql.functions import col, when, struct, array, explode, size, lit
from typing import List, Optional

def compare_delta_tables(table1: DataFrame, table2: DataFrame, join_key: str, include_columns: Optional[List[str]] = None, exclude_columns: Optional[List[str]] = None):
    """
    Compare two Delta tables and return a DataFrame with mismatches and counts.
    Works for both nested and non-nested structures.
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

    # Perform a full outer join
    joined_df = table1.alias("t1").join(table2.alias("t2"), on=join_key, how="full_outer")

    # Create comparison expressions
    compare_exprs = [
        when(col(f"t1.{c}") != col(f"t2.{c}"),
             struct(lit(c).alias("column"),
                    col(f"t1.{c}").alias("old_value"),
                    col(f"t2.{c}").alias("new_value"))
             )
        for c in compare_columns
    ]

    # Create an array of mismatches
    mismatches_array = array(*[expr for expr in compare_exprs])

    # Select join key, mismatches array, and calculate counts
    result_df = joined_df.select(
        col(f"t1.{join_key}").alias(join_key),
        mismatches_array.alias("mismatches"),
        size(mismatches_array).alias("mismatch_count"),
        (lit(len(compare_columns)) - size(mismatches_array)).alias("match_count")
    )

    # Filter out rows with no mismatches and explode the mismatches
    result_df = result_df.filter(col("mismatch_count") > 0).select(
        join_key,
        "mismatch_count",
        "match_count",
        explode("mismatches").alias("mismatch")
    ).select(
        join_key,
        "mismatch_count",
        "match_count",
        col("mismatch.column").alias("mismatched_column"),
        col("mismatch.old_value").alias("old_value"),
        col("mismatch.new_value").alias("new_value")
    )

    return result_df