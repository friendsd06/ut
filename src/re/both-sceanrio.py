from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, lit, struct, map_from_entries, expr, array
from pyspark.sql.types import StructType, ArrayType
from typing import List, Optional

def generate_comparison_expr(col_name, data_type):
    """
    Generate a comparison expression for a given column and data type.
    """
    if isinstance(data_type, StructType):
        nested_comparisons = [
            generate_comparison_expr(f"{col_name}.{field.name}", field.dataType)
            for field in data_type.fields
        ]
        return expr(f"map_from_entries(array({','.join(nested_comparisons)}))")
    elif isinstance(data_type, ArrayType):
        return expr(f"when(to_json(t1.{col_name}) != to_json(t2.{col_name}), "
                    f"struct(to_json(t1.{col_name}) as old, to_json(t2.{col_name}) as new))")
    else:
        return expr(f"when(t1.{col_name} != t2.{col_name}, "
                    f"struct(t1.{col_name} as old, t2.{col_name} as new))")

def compare_delta_tables(table1, table2, join_key: str, include_columns: Optional[List[str]] = None, exclude_columns: Optional[List[str]] = None):
    """
    Compare two Delta tables and return a DataFrame with the differences.
    Dynamically handles nested and non-nested schemas.
    """
    # Determine columns for comparison
    all_columns = set(table1.columns).intersection(set(table2.columns)) - {join_key}
    if include_columns:
        compare_columns = set(include_columns).intersection(all_columns)
    else:
        compare_columns = all_columns
    if exclude_columns:
        compare_columns = compare_columns - set(exclude_columns)

    # Perform a full outer join
    joined_df = table1.alias("t1").join(table2.alias("t2"), join_key, "full_outer")

    # Generate comparison expressions
    comparison_exprs = [
        (lit(col_name), generate_comparison_expr(col_name, table1.schema[col_name].dataType))
        for col_name in compare_columns
    ]

    # Create mismatch summary
    mismatch_summary = map_from_entries(array(*comparison_exprs))

    # Calculate mismatch count
    mismatch_count = sum([
        when(expr(f"element_at(mismatch_summary, '{col_name}') is not null"), 1).otherwise(0)
        for col_name in compare_columns
    ])

    # Select final columns and filter out rows with no mismatches
    result_df = joined_df.select(
        col(join_key),
        mismatch_summary.alias("mismatch_summary"),
        mismatch_count.alias("mismatch_count")
    ).filter(mismatch_count > 0)

    return result_df

# Test scenarios
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
        (3, "Charlie", {"age": 35, "scores": [90, 95, 100]}, 60000),
        (4, "David", {"age": 28, "scores": [75, 80, 85]}, 55000)
    ]
    data2 = [
        (1, "Alice", {"age": 31, "scores": [85, 90, 95]}, 50000),
        (2, "Bob", {"age": 25, "scores": [80, 85, 90, 95]}, 46000),
        (3, "Charlie", {"age": 35, "scores": [90, 95, 100]}, 60000),
        (5, "Eve", {"age": 22, "scores": [70, 75, 80]}, 40000)
    ]

    table1 = spark.createDataFrame(data1, mixed_schema)
    table2 = spark.createDataFrame(data2, mixed_schema)

    result = compare_delta_tables(table1, table2, join_key="id")
    result.show(truncate=False)
    print(result.count(), "rows with differences")

# Run test scenario
if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName("Delta Table Comparison Tests") \
        .getOrCreate()

    test_mixed_schema_scenario(spark)

    spark.stop()