from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, lit, expr, concat_ws, size, array, create_map, to_json
from pyspark.sql.types import StructType, ArrayType
from delta import DeltaTable

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

# Initialize SparkSession with S3 and Delta Lake configurations
spark = SparkSession.builder \
    .appName("S3 and Databricks Delta Table Comparison") \
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

# Parameters
include_columns = []  # Specify columns to include; leave empty for all
exclude_columns = []  # Columns to exclude, if any

# Define the join key (assuming 'id' is the common column, adjust as needed)
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
comparison_df = table1.alias("t1").join(table2.alias("t2"), join_key, "full_outer")

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

# Optionally, write the results to a Delta table
result_table_name = "comparison_results"
comparison_df.write.format("delta").mode("overwrite").saveAsTable(result_table_name)

print(f"Comparison results saved to Delta table: {result_table_name}")