from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, lit, expr, concat_ws, to_json, from_json, struct, array, map_from_entries
from pyspark.sql.types import StructType, ArrayType, DataType

def compare_columns(df, col1, col2, column_name):
    """
    Compare two columns and return a struct with the column name and difference.
    """
    if isinstance(col1.dataType, StructType):
        # For struct types, compare each field recursively
        field_comparisons = [compare_columns(df, col1[field.name], col2[field.name], f"{column_name}.{field.name}")
                             for field in col1.dataType.fields]
        return struct(*field_comparisons)
    elif isinstance(col1.dataType, ArrayType):
        # For array types, convert to json for comparison
        return when(to_json(col1) != to_json(col2),
                    struct(lit(column_name).alias("name"),
                           concat_ws(" -> ", to_json(col1), to_json(col2)).alias("diff")))
    else:
        # For other types, direct comparison
        return when(col1 != col2,
                    struct(lit(column_name).alias("name"),
                           concat_ws(" -> ", col1.cast("string"), col2.cast("string")).alias("diff")))

def compare_dataframes(df1, df2, join_key, include_columns=None, exclude_columns=None):
    """
    Compare two DataFrames and return a DataFrame with comparison results.
    Optimized for DataFrames with a large number of columns.
    """
    # Determine columns for comparison
    all_columns = set(df1.columns).intersection(set(df2.columns))
    if include_columns:
        compare_columns_list = list(set(include_columns).intersection(all_columns))
    else:
        compare_columns_list = list(all_columns)
    if exclude_columns:
        compare_columns_list = [col for col in compare_columns_list if col not in exclude_columns]
    compare_columns_list = [col for col in compare_columns_list if col != join_key]

    # Perform a full outer join
    joined_df = df1.alias("df1").join(df2.alias("df2"), join_key, "full_outer")

    # Generate mismatch information for all columns in one go
    mismatch_array = array(*[compare_columns(joined_df, col(f"df1.{column}"), col(f"df2.{column}"), column)
                             for column in compare_columns_list])

    # Create a map of mismatches
    mismatch_map = map_from_entries(mismatch_array.alias("mismatches"))

    # Add mismatch information and count
    result_df = joined_df.select(
        col(join_key),
        mismatch_map.alias("mismatches"),
        expr("size(filter(map_values(mismatches), x -> x is not null))").alias("mismatch_count")
    ).filter(col("mismatch_count") > 0)

    return result_df

# Initialize SparkSession with S3 and Delta Lake configurations
spark = SparkSession.builder \
    .appName("Large-Scale S3 and Databricks Delta Table Comparison") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.access.key", "YOUR_AWS_ACCESS_KEY") \
    .config("spark.hadoop.fs.s3a.secret.key", "YOUR_AWS_SECRET_KEY") \
    .getOrCreate()

# Read data from S3 in Delta format
s3_path = "s3a://your-bucket-name/path/to/delta/table"
table1 = spark.read.format("delta").load(s3_path)

# Read data from Databricks Delta table
databricks_table_name = "your_database.your_table_name"
table2 = spark.read.table(databricks_table_name)

# Define the join key (adjust as needed)
join_key = "id"

# Optional: specify columns to include or exclude
include_columns = []  # Leave empty to include all columns
exclude_columns = []  # Columns to exclude from comparison

# Perform the comparison
comparison_result = compare_dataframes(table1, table2, join_key, include_columns, exclude_columns)

# Display the results
comparison_result.show(truncate=False)

# Optionally, write the results to a Delta table
result_table_name = "comparison_results"
comparison_result.write.format("delta").mode("overwrite").saveAsTable(result_table_name)

print(f"Comparison results saved to Delta table: {result_table_name}")