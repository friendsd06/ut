from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, lit, struct, to_json, create_map
from pyspark.sql.types import StructType, ArrayType
from typing import List, Optional

def is_nested_column(data_type):
    """
    Check if a column is nested (struct or array type).
    """
    return isinstance(data_type, (StructType, ArrayType))

def compare_columns(col1, col2, col_name, data_type):
    """
    Compare two columns and return a column with differences.
    Handles both nested and non-nested types dynamically.
    """
    if is_nested_column(data_type):
        if isinstance(data_type, StructType):
            # For struct types, compare each field
            field_comparisons = [
                compare_columns(col1[field.name], col2[field.name], f"{col_name}.{field.name}", field.dataType)
                for field in data_type.fields
            ]
            return create_map(*[item for sublist in field_comparisons for item in sublist])
        elif isinstance(data_type, ArrayType):
            # For array types, compare as json strings
            return when(to_json(col1) != to_json(col2),
                        struct(to_json(col1).alias("old"), to_json(col2).alias("new"))
                        ).otherwise(None).alias(col_name)
    else:
        # For non-nested types, compare directly
        return when(col1 != col2, struct(col1.alias("old"), col2.alias("new"))).otherwise(None).alias(col_name)

def compare_delta_tables(table1, table2, join_key: str, include_columns: Optional[List[str]] = None, exclude_columns: Optional[List[str]] = None):
    """
    Compare two Delta tables and return a DataFrame with the differences.
    Dynamically handles nested and non-nested schemas.

    :param table1: First Delta table
    :param table2: Second Delta table
    :param join_key: Column name to use as the join key
    :param include_columns: List of columns to include in the comparison (None for all)
    :param exclude_columns: List of columns to exclude from the comparison
    :return: DataFrame with comparison results
    """
    # Determine columns for comparison
    all_columns = set(table1.columns).intersection(set(table2.columns)) - {join_key}
    if include_columns:
        compare_columns_set = set(include_columns).intersection(all_columns)
    else:
        compare_columns_set = all_columns
    if exclude_columns:
        compare_columns_set = compare_columns_set - set(exclude_columns)

    # Perform a full outer join
    joined_df = table1.alias("t1").join(table2.alias("t2"), join_key, "full_outer")

    # Generate mismatch information for each column
    select_expr = [col(join_key)]
    mismatch_columns = []
    for column in compare_columns_set:
        data_type = table1.schema[column].dataType
        mismatch_col = f"{column}_mismatch"
        joined_df = joined_df.withColumn(
            mismatch_col,
            compare_columns(col(f"t1.{column}"), col(f"t2.{column}"), column, data_type)
        )
        select_expr.append(col(mismatch_col))
        mismatch_columns.append(mismatch_col)

    # Calculate mismatch count
    joined_df = joined_df.withColumn(
        "mismatch_count",
        sum([when(col(c).isNotNull(), 1).otherwise(0) for c in mismatch_columns])
    )

    # Select final columns and filter out rows with no mismatches
    result_df = joined_df.select(*select_expr, "mismatch_count") \
        .filter(col("mismatch_count") > 0) \
        .withColumn("mismatch_summary", to_json(struct(*mismatch_columns)))

    return result_df

# Test scenarios
def test_mixed_schema_scenario(spark):
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