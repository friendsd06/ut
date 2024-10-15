from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, when, struct, lit
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, ArrayType
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

    # Select final columns and add mismatch count
    result_df = joined_df.select(
        col(f"t1.{join_key}").alias(join_key),
        *compare_exprs
    ).withColumn(
        "mismatch_count", mismatch_count_expr
    ).filter(col("mismatch_count") > 0)

    return result_df

# Test scenario
def test_mixed_schema_scenario(spark):
    print("Testing Mixed Schema Scenario:")

    # Define the schema
    mixed_schema = StructType([
        StructField("id", IntegerType(), False),
        StructField("name", StringType(), False),
        StructField("details", StructType([
            StructField("age", IntegerType(), True),
            StructField("scores", ArrayType(IntegerType()), True)
        ]), True),
        StructField("salary", IntegerType(), True)
    ])

    # Sample data
    data1 = [
        (1, "Alice", {"age": 30, "scores": [85, 90, 95]}, 50000),
        (2, "Bob", {"age": 25, "scores": [80, 85, 90]}, 45000),
        (3, "Charlie", {"age": 35, "scores": [90, 95, 100]}, 60000)
    ]
    data2 = [
        (1, "Alice", {"age": 31, "scores": [85, 90, 95]}, 50000),
        (2, "Bob", {"age": 25, "scores": [80, 85, 90, 95]}, 46000),
        (3, "Charlie", {"age": 35, "scores": [90, 95, 100]}, 60000)
    ]

    # Create DataFrames
    table1 = spark.createDataFrame(data1, mixed_schema)
    table2 = spark.createDataFrame(data2, mixed_schema)

    # Ensure they are DataFrames
    assert isinstance(table1, DataFrame), "table1 is not a DataFrame"
    assert isinstance(table2, DataFrame), "table2 is not a DataFrame"

    # Compare tables
    result = compare_delta_tables(table1, table2, join_key="id")
    result.show(truncate=False)
    print(result.count(), "rows with differences")

# Run test scenario
if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName("Simple Delta Table Comparison") \
        .getOrCreate()

    test_mixed_schema_scenario(spark)

    spark.stop()
