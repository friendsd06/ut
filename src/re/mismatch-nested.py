from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, lit, expr, concat_ws, size, array, create_map, to_json
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, ArrayType

def compare_nested_columns(df, col1, col2):
    """
    Recursively compare nested columns and return a column with differences.
    """
    if isinstance(col1.dataType, StructType):
        nested_comparisons = [
            compare_nested_columns(df, col1[field.name], col2[field.name]).alias(field.name)
            for field in col1.dataType.fields
        ]
        return create_map(*[item for sublist in nested_comparisons for item in sublist])
    elif isinstance(col1.dataType, ArrayType):
        return when(size(col1) != size(col2), concat_ws(" -> ", to_json(col1), to_json(col2))) \
            .otherwise(None)
    else:
        return when(col1 != col2, concat_ws(" -> ", col1.cast("string"), col2.cast("string"))) \
            .otherwise(None)

# Initialize SparkSession
spark = SparkSession.builder \
    .appName("Advanced Delta Table Comparison with Nested Columns") \
    .getOrCreate()

# Create two sample DataFrames with nested structures
data1 = [
    (1, "A", {"nested": {"value": 10, "array": [1, 2, 3]}, "top_level": 100}),
    (2, "B", {"nested": {"value": 20, "array": [4, 5, 6]}, "top_level": 200}),
    (3, "C", {"nested": {"value": 30, "array": [7, 8, 9]}, "top_level": 300})
]
data2 = [
    (1, "A", {"nested": {"value": 10, "array": [1, 2, 3]}, "top_level": 150}),
    (2, "B", {"nested": {"value": 25, "array": [4, 5, 6, 7]}, "top_level": 200}),
    (3, "D", {"nested": {"value": 30, "array": [7, 8, 9]}, "top_level": 300})
]

# Corrected schema definition
schema = StructType([
    StructField("id", IntegerType(), False),
    StructField("column1", StringType(), False),
    StructField("complex_column", StructType([
        StructField("nested", StructType([
            StructField("value", IntegerType(), True),
            StructField("array", ArrayType(IntegerType()), True)
        ]), True),
        StructField("top_level", IntegerType(), True)
    ]), True)
])

table1 = spark.createDataFrame(data1, schema)
table2 = spark.createDataFrame(data2, schema)

# Parameters
include_columns = ["column1", "complex_column"]  # Specify columns to include; leave empty for all
exclude_columns = []  # Columns to exclude, if any

# Define the join key
join_key = "id"

# Determine columns for comparison
all_common_columns = [col for col in table1.columns if col in table2.columns and col != join_key]

# Apply include and exclude filters
if include_columns:
    common_columns = [col for col in all_common_columns if col in include_columns]
else:
    common_columns = all_common_columns
common_columns = [col for col in common_columns if col not in exclude_columns]

# Perform a join on the key column
comparison_df = table1.alias("t1").join(table2.alias("t2"), join_key, "inner")

# Generate mismatch information for each common column
for column in common_columns:
    comparison_df = comparison_df \
        .withColumn(f"{column}_mismatch",
                    compare_nested_columns(comparison_df, col(f"t1.{column}"), col(f"t2.{column}")))

# Create an array column with all mismatch columns to count mismatches
mismatch_columns = [f"{col}_mismatch" for col in common_columns]
comparison_df = comparison_df.withColumn("mismatch_array", array(*mismatch_columns))

# Calculate match and mismatch counts
comparison_df = comparison_df \
    .withColumn("mismatch_count", size(array(*[when(col(c).isNotNull(), lit(1)).otherwise(lit(0)) for c in mismatch_columns]))) \
    .withColumn("match_count", lit(len(common_columns)) - col("mismatch_count"))

# Concatenate all mismatches into a single summary column
comparison_df = comparison_df \
    .withColumn("mismatch_summary", to_json(create_map(*[item for sublist in
                                                         [(lit(c), col(c)) for c in mismatch_columns] for item in sublist]))) \
    .filter(col("mismatch_count") > 0) \
    .select(join_key, "match_count", "mismatch_count", "mismatch_summary", *mismatch_columns)

# Display the results
comparison_df.show(truncate=False)