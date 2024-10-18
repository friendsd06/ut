from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, concat, lit, concat_ws

# Initialize Spark session
spark = SparkSession.builder.appName("MismatchTest").getOrCreate()

# Sample data to test the mismatch detection
data = [
    ("key1", "A", "A"),       # No mismatch
    ("key2", "A", "B"),       # Value mismatch
    ("key3", None, "B"),      # Source is NULL, target is not
    ("key4", "A", None),      # Source is not NULL, target is NULL
    ("key5", None, None),     # Both are NULL (should not be a mismatch)
    ("key6", "", ""),         # Empty strings (no mismatch)
    ("key7", None, ""),       # Source is NULL, target is empty string
    ("key8", "", None),       # Source is empty string, target is NULL
]

columns = ["primary_key", "source_col", "target_col"]
value_mismatches = spark.createDataFrame(data, columns)

# Columns to compare
columns_to_compare_final = ["col"]

# Original code that has the issue
print("=== Original Code Output ===")
original_detailed_mismatches = value_mismatches.select(
    "primary_key",
    concat_ws(", ", *[
        when(
            (col(f"source_{c}").cast("string") != col(f"target_{c}").cast("string")) |
            (col(f"source_{c}").isNull() != col(f"target_{c}").isNull()),
            concat(
                lit(f"{c}: source="),
                col(f"source_{c}").cast("string"),
                lit(", target="),
                col(f"target_{c}").cast("string")
            )
        )
        for c in columns_to_compare_final
    ]).alias("differences")
).filter("differences IS NOT NULL")

original_detailed_mismatches.show(truncate=False)

# Corrected code using eqNullSafe
print("=== Corrected Code Output ===")
corrected_detailed_mismatches = value_mismatches.select(
    "primary_key",
    concat_ws(", ", *[
        when(
            ~col(f"source_{c}").cast("string").eqNullSafe(col(f"target_{c}").cast("string")),
            concat(
                lit(f"{c}: source="),
                when(col(f"source_{c}").isNull(), lit("NULL")).otherwise(col(f"source_{c}").cast("string")),
                lit(", target="),
                when(col(f"target_{c}").isNull(), lit("NULL")).otherwise(col(f"target_{c}").cast("string"))
            )
        )
        for c in columns_to_compare_final
    ]).alias("differences")
).filter("differences IS NOT NULL")

corrected_detailed_mismatches.show(truncate=False)

# Stop Spark session
spark.stop()
