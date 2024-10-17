from pyspark.sql import DataFrame
from pyspark.sql.functions import col, when, lit, struct, collect_list, size
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from typing import List, Optional

def compare_delta_tables(table1: DataFrame, table2: DataFrame, join_key: str, include_columns: Optional[List[str]] = None, exclude_columns: Optional[List[str]] = None):
    """
    Compare two Delta tables and return a DataFrame with mismatches and counts.
    Optimized for performance on large datasets.
    """
    # Ensure the inputs are DataFrames
    if not isinstance(table1, DataFrame) or not isinstance(table2, DataFrame):
        raise ValueError("Both table1 and table2 must be Spark DataFrames.")

    # Determine columns for comparison
    all_columns = set(table1.columns) & set(table2.columns) - {join_key}
    compare_columns = list(set(include_columns or all_columns) - set(exclude_columns or []))

    if not compare_columns:
        raise ValueError("No columns available for comparison after applying include/exclude filters.")

    # Perform a full outer join
    joined_df = table1.alias("t1").join(table2.alias("t2"), on=join_key, how="full_outer")

    # Create a single struct for all mismatches
    mismatch_struct = struct(*[
        when(col(f"t1.{c}") != col(f"t2.{c}"),
             struct(lit(c).alias("column"),
                    col(f"t1.{c}").alias("old_value"),
                    col(f"t2.{c}").alias("new_value"))
             ).alias(c)
        for c in compare_columns
    ])

    # Select join key and mismatch struct
    result_df = joined_df.select(
        col(f"t1.{join_key}").alias(join_key),
        mismatch_struct.alias("mismatches")
    )

    # Define a schema for the exploded mismatches
    mismatch_schema = StructType([
        StructField("column", StringType(), True),
        StructField("old_value", StringType(), True),
        StructField("new_value", StringType(), True)
    ])

    # Use a UDF to explode the struct and count mismatches
    @udf(ArrayType(mismatch_schema))
    def explode_mismatches(mismatches):
        return [row for row in mismatches.asDict().values() if row is not None]

    # Apply the UDF and add count columns
    result_df = result_df.withColumn("mismatch_list", explode_mismatches("mismatches"))
    result_df = result_df.withColumn("mismatch_count", size("mismatch_list"))
    result_df = result_df.withColumn("match_count", lit(len(compare_columns)) - col("mismatch_count"))

    # Filter rows with mismatches and select final columns
    final_df = result_df.filter(col("mismatch_count") > 0).select(
        join_key,
        "mismatch_count",
        "match_count",
        "mismatch_list"
    )

    return final_df