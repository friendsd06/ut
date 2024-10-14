from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, struct, lit
from typing import List, Optional

def compare_delta_tables(table1, table2, join_key: str, include_columns: Optional[List[str]] = None, exclude_columns: Optional[List[str]] = None):
    """
    Compare two Delta tables and return a DataFrame with the differences.
    Works for both nested and non-nested structures.
    """
    # Determine columns for comparison
    all_columns = set(table1.columns) & set(table2.columns) - {join_key}
    if include_columns:
        compare_columns = list(set(include_columns) & all_columns)
    else:
        compare_columns = list(all_columns)
    if exclude_columns:
        compare_columns = [col for col in compare_columns if col not in exclude_columns]

    # Perform a full outer join
    joined_df = table1.alias("t1").join(table2.alias("t2"), join_key, "full_outer")

    # Create comparison expressions
    compare_exprs = [
        when(col(f"t1.{c}") != col(f"t2.{c}"),
             struct(col(f"t1.{c}").alias("old"), col(f"t2.{c}").alias("new"))
             ).alias(c)
        for c in compare_columns
    ]

    # Select final columns and add mismatch count
    result_df = joined_df.select(
        col(f"t1.{join_key}").alias(join_key),
        *compare_exprs
    ).withColumn(
        "mismatch_count",
        sum([when(col(c).isNotNull(), lit(1)).otherwise(lit(0)) for c in compare_columns])
    ).filter(col("mismatch_count") > 0)

    return result_df

# Test scenario
def test_mixed_schema_scenario(spark):
    from pyspark.sql.types import StructType, StructField, IntegerType, StringType, ArrayType

    print("Testing Mixed Schema Scenario:")

    mixed_schema = StructType([
        StructField("id", IntegerType(), False),
        StructField("name", StringType(), False),
        StructField("details", StructType([
            StructField("age", IntegerType(), True),
            StructField("scores", ArrayType(IntegerType()), True)
        ]), True),
        StructField("salary", IntegerType(), True)
    ])

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

    table1 = spark.createDataFrame(data1, mixed_schema)
    table2 = spark.createDataFrame(data2, mixed_schema)

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