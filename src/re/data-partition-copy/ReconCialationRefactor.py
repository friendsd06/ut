from pyspark.sql import SparkSession, DataFrame
import pyspark.sql.functions as F
from typing import List, Optional, Union

def reconcile_dataframes(
        source_df: DataFrame,
        target_df: DataFrame,
        join_keys: Union[str, List[str]],
        columns_to_compare: Optional[List[str]] = None,
        columns_to_include: Optional[List[str]] = None,
        columns_to_ignore: Optional[List[str]] = None
) -> DataFrame:
    if not isinstance(source_df, DataFrame) or not isinstance(target_df, DataFrame):
        raise ValueError("Both source_df and target_df must be Spark DataFrames.")

    # Convert join_keys to a list if it's a string
    join_keys = [join_keys] if isinstance(join_keys, str) else join_keys

    # Validate join keys
    if not all(key in source_df.columns and key in target_df.columns for key in join_keys):
        raise ValueError("All join keys must be present in both source and target DataFrames.")

    common_columns = list(set(source_df.columns) & set(target_df.columns) - set(join_keys))

    if columns_to_include:
        columns_for_comparison = [c for c in columns_to_include if c in common_columns]
        if not columns_for_comparison:
            raise ValueError("No columns to include are present in both DataFrames.")
    elif columns_to_compare:
        columns_for_comparison = [c for c in columns_to_compare if c in common_columns]
        if not columns_for_comparison:
            raise ValueError("No columns to compare are present in both DataFrames.")
    else:
        columns_for_comparison = common_columns

    if columns_to_ignore:
        columns_for_comparison = [c for c in columns_for_comparison if c not in columns_to_ignore]

    if not columns_for_comparison:
        raise ValueError("No columns left to compare after applying include and ignore lists.")

    # Select columns for comparison
    source_selected = source_df.select(*join_keys, *columns_for_comparison)
    target_selected = target_df.select(*join_keys, *columns_for_comparison)

    # Perform the join
    join_condition = [source_selected[key] == target_selected[key] for key in join_keys]
    joined_df = source_selected.join(
        target_selected,
        on=join_condition,
        how="full_outer"
    )

    # Create mismatch conditions
    mismatch_conditions = [
        (F.col(f"{c}") != F.col(f"{c}_2")) |
        (F.col(f"{c}").isNull() != F.col(f"{c}_2").isNull())
        for c in columns_for_comparison
    ]

    overall_mismatch_condition = F.reduce(lambda x, y: x | y, mismatch_conditions)

    # Filter mismatches
    mismatched_rows = joined_df.filter(overall_mismatch_condition).withColumn("mismatch_type", F.lit("Value Mismatch"))

    def create_mismatch_description(column):
        return F.when(
            F.col(f"{column}").cast("string").eqNullSafe(F.col(f"{column}_2").cast("string")),
            F.concat(
                F.lit(f"{column}: source="),
                F.coalesce(F.col(f"{column}").cast("string"), F.lit("NULL")),
                F.lit(", target="),
                F.coalesce(F.col(f"{column}_2").cast("string"), F.lit("NULL"))
            )
        ).otherwise(F.lit(None))

    mismatch_descriptions = [create_mismatch_description(column) for column in columns_for_comparison]

    # Create the final result
    reconciliation_results = mismatched_rows.select(
        *join_keys,
        F.array_remove(F.array(*mismatch_descriptions), None).alias("discrepancies")
    ).filter("size(discrepancies) > 0")

    return reconciliation_results

# Sample dataset creation
def create_sample_datasets(spark: SparkSession):
    source_data = [
        (1, "John", "Doe", 30, "New York"),
        (2, "Jane", "Smith", 28, "Los Angeles"),
        (3, "Mike", "Johnson", 35, "Chicago"),
        (4, "Emily", "Brown", 32, "Houston")
    ]

    target_data = [
        (1, "John", "Doe", 31, "New York"),  # Age mismatch
        (2, "Jane", "Smith", 28, "San Francisco"),  # City mismatch
        (3, "Michael", "Johnson", 35, "Chicago"),  # First name mismatch
        (5, "Sarah", "Wilson", 29, "Boston")  # New record in target
    ]

    source_schema = ["id", "first_name", "last_name", "age", "city"]
    target_schema = ["id", "first_name", "last_name", "age", "city"]

    source_df = spark.createDataFrame(source_data, schema=source_schema)
    target_df = spark.createDataFrame(target_data, schema=target_schema)

    return source_df, target_df

# Test the function
if __name__ == "__main__":
    spark = SparkSession.builder.appName("ReconciliationTest").getOrCreate()

    source_df, target_df = create_sample_datasets(spark)

    print("Source DataFrame:")
    source_df.show()

    print("Target DataFrame:")
    target_df.show()

    result = reconcile_dataframes(
        source_df,
        target_df,
        join_keys="id",
        columns_to_compare=["first_name", "last_name", "age", "city"]
    )

    print("Reconciliation Results:")
    result.show(truncate=False)

    spark.stop()