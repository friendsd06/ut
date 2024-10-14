from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, lit, expr, concat_ws, size, array, create_map, to_json
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, ArrayType

def compare_columns(df, col1, col2):
    """
    Compare two columns and return a column with differences.
    Works for both nested and non-nested structures.
    """
    if isinstance(col1.dataType, StructType):
        nested_comparisons = [
            compare_columns(df, col1[field.name], col2[field.name]).alias(field.name)
            for field in col1.dataType.fields
        ]
        return create_map(*[item for sublist in nested_comparisons for item in sublist])
    elif isinstance(col1.dataType, ArrayType):
        return when(size(col1) != size(col2), concat_ws(" -> ", to_json(col1), to_json(col2))) \
            .otherwise(None)
    else:
        return when(col1 != col2, concat_ws(" -> ", col1.cast("string"), col2.cast("string"))) \
            .otherwise(None)

def compare_delta_tables(table1, table2, join_key, include_columns=None, exclude_columns=None):
    """
    Compare two Delta tables and return a DataFrame with the differences.
    """
    all_common_columns = [col for col in table1.columns if col in table2.columns and col != join_key]

    if include_columns:
        common_columns = [col for col in all_common_columns if col in include_columns]
    else:
        common_columns = all_common_columns

    if exclude_columns:
        common_columns = [col for col in common_columns if col not in exclude_columns]

    comparison_df = table1.alias("t1").join(table2.alias("t2"), join_key, "inner")

    for column in common_columns:
        comparison_df = comparison_df.withColumn(
            f"{column}_mismatch",
            compare_columns(comparison_df, col(f"t1.{column}"), col(f"t2.{column}"))
        )

    mismatch_columns = [f"{col}_mismatch" for col in common_columns]

    comparison_df = comparison_df \
        .withColumn("mismatch_count", size(array(*[when(col(c).isNotNull(), lit(1)).otherwise(lit(0)) for c in mismatch_columns]))) \
        .withColumn("match_count", lit(len(common_columns)) - col("mismatch_count"))

    comparison_df = comparison_df \
        .withColumn("mismatch_summary", to_json(create_map(*[item for sublist in
                                                             [(lit(c), col(c)) for c in mismatch_columns] for item in sublist]))) \
        .filter(col("mismatch_count") > 0) \
        .select(join_key, "match_count", "mismatch_count", "mismatch_summary", *mismatch_columns)

    return comparison_df

# Initialize SparkSession
spark = SparkSession.builder \
    .appName("Delta Table Comparison Tests") \
    .getOrCreate()

# Scenario 1: Nested Structure
def test_nested_scenario():
    print("Testing Nested Structure Scenario:")

    # Schema for nested structure
    nested_schema = StructType([
        StructField("id", IntegerType(), False),
        StructField("name", StringType(), False),
        StructField("details", StructType([
            StructField("age", IntegerType(), True),
            StructField("scores", ArrayType(IntegerType()), True)
        ]), True)
    ])

    # Data for nested structure
    data1 = [
        (1, "Alice", {"age": 30, "scores": [85, 90, 95]}),
        (2, "Bob", {"age": 25, "scores": [80, 85, 90]}),
        (3, "Charlie", {"age": 35, "scores": [90, 95, 100]})
    ]
    data2 = [
        (1, "Alice", {"age": 31, "scores": [85, 90, 95]}),
        (2, "Bob", {"age": 25, "scores": [80, 85, 90, 95]}),
        (3, "Charlie", {"age": 35, "scores": [90, 95, 100]})
    ]

    table1 = spark.createDataFrame(data1, nested_schema)
    table2 = spark.createDataFrame(data2, nested_schema)

    result = compare_delta_tables(table1, table2, join_key="id")
    result.show(truncate=False)

# Scenario 2: Non-Nested Structure
def test_non_nested_scenario():
    print("\nTesting Non-Nested Structure Scenario:")

    # Schema for non-nested structure
    non_nested_schema = StructType([
        StructField("id", IntegerType(), False),
        StructField("name", StringType(), False),
        StructField("age", IntegerType(), True),
        StructField("salary", IntegerType(), True)
    ])

    # Data for non-nested structure
    data1 = [
        (1, "Alice", 30, 50000),
        (2, "Bob", 25, 45000),
        (3, "Charlie", 35, 60000)
    ]
    data2 = [
        (1, "Alice", 31, 50000),
        (2, "Bob", 25, 46000),
        (3, "Charlie", 35, 60000)
    ]

    table1 = spark.createDataFrame(data1, non_nested_schema)
    table2 = spark.createDataFrame(data2, non_nested_schema)

    result = compare_delta_tables(table1, table2, join_key="id")
    result.show(truncate=False)

# Run both scenarios
if __name__ == "__main__":
    test_nested_scenario()
    test_non_nested_scenario()