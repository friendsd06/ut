from pyspark.sql import DataFrame
from pyspark.sql.functions import col, when, struct, lit
from typing import List, Optional
from functools import reduce

def compare_delta_tables(table1: DataFrame, table2: DataFrame, join_key: str, include_columns: Optional[List[str]] = None, exclude_columns: Optional[List[str]] = None):
    """
    Compare two Delta tables and return a DataFrame with the differences.
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
             struct(col(f"t1.{c}").alias("old"), col(f"t2.{c}").alias("new"))
             ).alias(c)
        for c in compare_columns
    ]

    # Calculate mismatch count using reduce to sum mismatches
    mismatch_count_expr = reduce(
        lambda acc, c: acc + when(col(c).isNotNull(), lit(1)).otherwise(lit(0)),
        compare_columns,
        lit(0)
    )

    # Calculate match count
    match_count_expr = lit(len(compare_columns)) - mismatch_count_expr

    # Select final columns, add mismatch and match counts, and filter for mismatches
    result_df = joined_df.select(
        col(f"t1.{join_key}").alias(join_key),
        *compare_exprs
    ).withColumn(
        "mismatch_count", mismatch_count_expr
    ).withColumn(
        "match_count", match_count_expr
    ).filter(col("mismatch_count") > 0)

    # Keep only mismatched columns
    mismatched_columns = [c for c in compare_columns if c in result_df.columns]
    final_columns = [join_key, "mismatch_count", "match_count"] + mismatched_columns

    return result_df.select(*final_columns)