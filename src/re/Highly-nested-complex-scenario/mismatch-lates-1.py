from pyspark.sql import DataFrame
from pyspark.sql.functions import col, when, lit, sum as sum_
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

    # Create comparison expressions and mismatch count
    compare_exprs = []
    mismatch_count_expr = lit(0)

    for c in compare_columns:
        is_mismatch = (col(f"t1.{c}") != col(f"t2.{c}"))
        compare_exprs.append(
            when(is_mismatch, lit(c)).alias(f"mismatch_{c}")
        )
        compare_exprs.extend([
            when(is_mismatch, col(f"t1.{c}")).alias(f"old_{c}"),
            when(is_mismatch, col(f"t2.{c}")).alias(f"new_{c}")
        ])
        mismatch_count_expr += when(is_mismatch, lit(1)).otherwise(lit(0))

    # Select columns and add mismatch/match counts
    result_df = joined_df.select(
        col(f"t1.{join_key}").alias(join_key),
        *compare_exprs,
        mismatch_count_expr.alias("mismatch_count")
    ).withColumn("match_count", lit(len(compare_columns)) - col("mismatch_count"))

    # Filter rows with mismatches
    result_df = result_df.filter(col("mismatch_count") > 0)

    return result_df