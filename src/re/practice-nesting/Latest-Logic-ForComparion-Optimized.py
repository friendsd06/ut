from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col, when, lit, count, coalesce, concat_ws, concat
)
from typing import List, Optional, Tuple
from functools import reduce
import pyspark.sql.functions as F

def compare_delta_tables(
        source_df: DataFrame,
        target_df: DataFrame,
        primary_key: str,
        columns_to_compare: Optional[List[str]] = None,
        columns_to_include: Optional[List[str]] = None,
        columns_to_ignore: Optional[List[str]] = None
) -> Tuple[DataFrame, DataFrame]:
    """
    Compare two Delta tables and return detailed mismatches and summary statistics.

    Args:
        source_df (DataFrame): The source DataFrame to compare from.
        target_df (DataFrame): The target DataFrame to compare to.
        primary_key (str): The column name to use as the primary key for joining.
        columns_to_compare (List[str], optional): Specific columns to compare.
            If None, compares all common columns excluding the primary key.
        columns_to_include (List[str], optional): Specific columns to include in the comparison.
            Overrides columns_to_compare if provided.
        columns_to_ignore (List[str], optional): Specific columns to exclude from the comparison.

    Returns:
        tuple: (detailed_mismatches_df, summary_df) - Detailed mismatches and summary statistics.
    """

    # Validate inputs
    if not isinstance(source_df, DataFrame) or not isinstance(target_df, DataFrame):
        raise ValueError("Both source_df and target_df must be Spark DataFrames.")

    # Determine common columns excluding primary key
    common_columns = list(set(source_df.columns) & set(target_df.columns) - {primary_key})

    # Apply columns_to_include
    if columns_to_include:
        # Ensure included columns are present in both DataFrames
        columns_to_include = [c for c in columns_to_include if c in common_columns]
        if not columns_to_include:
            raise ValueError("No columns to include are present in both DataFrames.")
        columns_to_compare_final = columns_to_include
    elif columns_to_compare:
        # Use specified columns_to_compare if provided
        columns_to_compare_final = [c for c in columns_to_compare if c in common_columns]
        if not columns_to_compare_final:
            raise ValueError("No columns to compare are present in both DataFrames.")
    else:
        # Default to all common columns
        columns_to_compare_final = common_columns

    # Apply columns_to_ignore
    if columns_to_ignore:
        columns_to_compare_final = [c for c in columns_to_compare_final if c not in columns_to_ignore]

    if not columns_to_compare_final:
        raise ValueError("No columns left to compare after applying include and ignore lists.")

    # Select relevant columns and alias them for clarity
    source_selected = source_df.select(
        [col(primary_key)] + [col(c).alias(f"source_{c}") for c in columns_to_compare_final]
    )
    target_selected = target_df.select(
        [col(primary_key)] + [col(c).alias(f"target_{c}") for c in columns_to_compare_final]
    )

    # Repartition DataFrames to optimize join performance
    num_partitions = 500  # Adjust based on cluster size and data distribution
    source_repartitioned = source_selected.repartition(num_partitions, primary_key).cache()
    target_repartitioned = target_selected.repartition(num_partitions, primary_key).cache()

    # Perform full outer join on primary key
    joined_df = source_repartitioned.join(
        target_repartitioned, on=primary_key, how="full_outer"
    )

    # Identify rows missing in source or target
    missing_in_source = joined_df.filter(col(f"target_{primary_key}").isNull()) \
        .withColumn("mismatch_type", lit("Missing in Source"))
    missing_in_target = joined_df.filter(col(f"source_{primary_key}").isNull()) \
        .withColumn("mismatch_type", lit("Missing in Target"))

    # Identify rows with value mismatches
    mismatch_conditions = [
        (col(f"source_{c}").cast("string") != col(f"target_{c}").cast("string")) |
        (col(f"source_{c}").isNull() != col(f"target_{c}").isNull())
        for c in columns_to_compare_final
    ]

    overall_mismatch_condition = reduce(lambda x, y: x | y, mismatch_conditions)

    value_mismatches = joined_df.filter(overall_mismatch_condition) \
        .withColumn("mismatch_type", lit("Value Mismatch"))

    # Combine all mismatch types
    mismatch_df = missing_in_source.union(missing_in_target).union(value_mismatches)

    # Create detailed mismatch descriptions for value mismatches
    detailed_mismatches = value_mismatches.select(
        primary_key,
        "mismatch_type",
        concat_ws(", ", *[
            when(
                (col(f"source_{c}").cast("string") != col(f"target_{c}").cast("string")) |
                (col(f"source_{c}").isNull() != col(f"target_{c}").isNull()),
                concat(
                    lit(f"{c}: source="),
                    col(f"source_{c}").cast("string"),
                    lit(", target="),
                    col(f"target_{c}").cast("string")
                )
            )
            for c in columns_to_compare_final
        ]).alias("differences")
    ).filter("differences IS NOT NULL")

    # Summary statistics
    # Use aggregations in a single pass to minimize actions
    summary_df = mismatch_df.agg(
        count(when(col("mismatch_type") == "Missing in Target", True)).alias("missing_in_target_count"),
        count(when(col("mismatch_type") == "Missing in Source", True)).alias("missing_in_source_count"),
        count(when(col("mismatch_type") == "Value Mismatch", True)).alias("value_mismatch_count"),
    )

    # Calculate total records in source and target
    # Use distinct counts to ensure uniqueness
    total_source_records = source_repartitioned.select(primary_key).distinct().count()
    total_target_records = target_repartitioned.select(primary_key).distinct().count()

    # Add total records and matched records to summary
    summary_df = summary_df.withColumn("total_source_records", lit(total_source_records)) \
        .withColumn("total_target_records", lit(total_target_records)) \
        .withColumn(
        "matched_records_count",
        lit(total_source_records) - col("missing_in_target_count") - col("value_mismatch_count")
    )

    return detailed_mismatches, summary_df