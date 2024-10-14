from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, lit, expr, concat_ws, size, array, filter
from pyspark.sql.types import BooleanType

# Initialize SparkSession
spark = SparkSession.builder \
    .appName("Delta Table Comparison Test with Custom Data") \
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
columns = ["id", "column1", "column2", "column3"]
table1 = spark.createDataFrame(data1, columns)
table2 = spark.createDataFrame(data2, columns)

# Parameters
include_columns = ["column1", "column2", "column3"]  # Specify columns to include; leave empty for all
exclude_columns = []  # Columns to exclude, if any

# Define the join key (assuming 'id' is the common column)
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
                    when(col(f"t1.{column}") != col(f"t2.{column}"),
                         concat_ws(" -> ", col(f"t1.{column}"), col(f"t2.{column}")))
                    .otherwise(lit(None)))

# Create an array column with all mismatch columns to count mismatches
mismatch_columns = [f"{col}_mismatch" for col in common_columns]
comparison_df = comparison_df.withColumn("mismatch_array", array(*mismatch_columns))

# Calculate match and mismatch counts
comparison_df = comparison_df \
    .withColumn("mismatch_count", size(filter("mismatch_array", lambda x: x.isNotNull()))) \
    .withColumn("match_count", lit(len(common_columns)) - col("mismatch_count"))

# Concatenate all mismatches into a single summary column
mismatch_summary_expr = " , ".join(mismatch_columns)
comparison_df = comparison_df \
    .withColumn("mismatch_summary", expr(f"concat_ws(',', {mismatch_summary_expr})")) \
    .filter(col("mismatch_summary").isNotNull()) \
    .select(join_key, "match_count", "mismatch_count", "mismatch_summary", *mismatch_columns)

# Display the results
comparison_df.show(truncate=False)