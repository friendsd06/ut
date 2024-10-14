from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, concat_ws, lit, array, size, filter, expr

# Initialize SparkSession
spark = SparkSession.builder \
    .appName("Simple Delta Table Comparison with Custom Data") \
    .getOrCreate()

# Create two sample DataFrames
data1 = [
    (1, "A", 10, 100),
    (2, "B", 20, 200),
    (3, "C", 30, 300)
]
data2 = [
    (1, "A", 10, 150),
    (2, "B", 25, 200),
    (3, "D", 30, 300)
]

# Define columns for DataFrames
columns = ["id", "column1", "column2", "column3"]

# Create DataFrames
table1 = spark.createDataFrame(data1, columns)
table2 = spark.createDataFrame(data2, columns)

# Define columns to include/exclude and join key
include_columns = ["column1", "column2", "column3"]
exclude_columns = []  # Keep this empty if no columns are to be excluded
join_key = "id"

# Identify common columns between both tables, excluding the join key
all_common_columns = [col for col in table1.columns if col in table2.columns and col != join_key]

# Filter columns based on include/exclude lists
common_columns = [col for col in all_common_columns if col in include_columns]
common_columns = [col for col in common_columns if col not in exclude_columns]

# Perform a join on the specified join key
comparison_df = table1.join(table2, on=join_key, how="inner").select(
    table1.id,
    *(col(f"table1.{col}").alias(f"table1_{col}") for col in common_columns),
    *(col(f"table2.{col}").alias(f"table2_{col}") for col in common_columns)
)

# Initialize mismatch columns and mismatch arrays for storing mismatches
for column in common_columns:
    comparison_df = comparison_df.withColumn(
        f"{column}_mismatch",
        when(col(f"table1_{column}") != col(f"table2_{column}"),
             concat_ws(" -> ", col(f"table1_{column}"), col(f"table2_{column}"))
             ).otherwise(lit(None))
    )

# Create an array of all mismatch columns for mismatch count
mismatch_columns = [f"{col}_mismatch" for col in common_columns]
comparison_df = comparison_df.withColumn("mismatch_array", array(*mismatch_columns))

# Count mismatches by filtering non-null elements in the mismatch array
comparison_df = comparison_df.withColumn("mismatch_count", size(filter(col("mismatch_array"), lambda x: x.isNotNull())))

# Calculate match count based on total columns - mismatch count
comparison_df = comparison_df.withColumn("match_count", lit(len(common_columns)) - col("mismatch_count"))

# Concatenate mismatches into a summary string
mismatch_summary_expr = ", ".join(mismatch_columns)
comparison_df = comparison_df.withColumn("mismatch_summary", expr(f"concat_ws(', ', {mismatch_summary_expr})"))

# Select relevant columns for final output
final_columns = [join_key, "match_count", "mismatch_count", "mismatch_summary"] + mismatch_columns
result_df = comparison_df.select(*final_columns)

# Display results
result_df.show(truncate=False)
