# Import necessary libraries
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import explode, col, when, concat, lit, concat_ws, broadcast
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, ArrayType

# Initialize SparkSession
spark = SparkSession.builder \
    .appName("NestedDataFrameReconciliation") \
    .getOrCreate()

# For display purposes in notebooks (optional)
spark.sparkContext.setLogLevel("ERROR")

# Define schema for source DataFrame
schema_source = StructType([
    StructField("main_id", IntegerType(), True),
    StructField("main_attr1", StringType(), True),
    StructField("main_attr2", StringType(), True),
    StructField("nested_entities", ArrayType(StructType([
        StructField("nested_id", IntegerType(), True),
        StructField("nested_attr1", StringType(), True),
        StructField("nested_attr2", StringType(), True)
    ])), True)
])

# Define schema for target DataFrame
schema_target = StructType([
    StructField("main_id", IntegerType(), True),
    StructField("main_attr1", StringType(), True),
    StructField("main_attr2", StringType(), True),
    StructField("nested_entities", ArrayType(StructType([
        StructField("nested_id", IntegerType(), True),
        StructField("nested_attr1", StringType(), True),
        StructField("nested_attr2", StringType(), True)
    ])), True)
])

# Sample data for source DataFrame
data_source = [
    (1, "MainA", "Active", [
        {"nested_id": 101, "nested_attr1": "NA1", "nested_attr2": "NY1"},
        {"nested_id": 102, "nested_attr1": "NA2", "nested_attr2": "NY2"}
    ]),
    (2, "MainB", "Active", [
        {"nested_id": 103, "nested_attr1": "NB1", "nested_attr2": "NY3"}
    ]),
    (3, "MainC", "Inactive", [
        {"nested_id": 104, "nested_attr1": "NC1", "nested_attr2": "NY4"}
    ])
]

# Sample data for target DataFrame
data_target = [
    (1, "MainA", "Active", [
        {"nested_id": 101, "nested_attr1": "NA1", "nested_attr2": "NY1"},  # No mismatch
        {"nested_id": 102, "nested_attr1": "NA2_modified", "nested_attr2": "NY2"}  # nested_attr1 mismatch
    ]),
    (2, "MainB", "Active", [
        {"nested_id": 103, "nested_attr1": "NB1", "nested_attr2": "NY3"},
        {"nested_id": 105, "nested_attr1": "NB2", "nested_attr2": "NY5"}  # New nested entity
    ]),
    (4, "MainD", "Active", [  # New main entity
        {"nested_id": 106, "nested_attr1": "ND1", "nested_attr2": "NY6"}
    ])
]

# Create DataFrames
source_df = spark.createDataFrame(data_source, schema_source)
target_df = spark.createDataFrame(data_target, schema_target)

# Display source and target DataFrames
print("=== Source DataFrame ===")
source_df.show(truncate=False)

print("\n=== Target DataFrame ===")
target_df.show(truncate=False)

# Function to flatten nested entities
def flatten_nested_entities(df: DataFrame, nested_col: str) -> DataFrame:
    """
    Flattens a DataFrame by exploding a nested array of structs.

    Args:
        df (DataFrame): The input DataFrame with nested structures.
        nested_col (str): The name of the nested column to explode.

    Returns:
        DataFrame: A flattened DataFrame with one row per nested entity.
    """
    return df.select(
        "main_id",
        "main_attr1",
        "main_attr2",
        explode(col(nested_col)).alias("nested_entity")
    ).select(
        "main_id",
        "main_attr1",
        "main_attr2",
        col("nested_entity.nested_id").alias("nested_id"),
        col("nested_entity.nested_attr1").alias("nested_attr1"),
        col("nested_entity.nested_attr2").alias("nested_attr2")
    )

# Flatten both DataFrames
source_flat = flatten_nested_entities(source_df, "nested_entities")
target_flat = flatten_nested_entities(target_df, "nested_entities")

# Display flattened DataFrames
print("=== Source Flattened DataFrame ===")
source_flat.show(truncate=False)

print("\n=== Target Flattened DataFrame ===")
target_flat.show(truncate=False)

# Reconciliation of Top-Level Entities
# Perform a full outer join on main_id for top-level reconciliation
joined_main = source_df.alias("source").join(
    target_df.alias("target"),
    on="main_id",
    how="full_outer"
)

# Compare top-level attributes
main_differences = joined_main.select(
    "main_id",
    when(
        (col("source.main_attr1") != col("target.main_attr1")) |
        (col("source.main_attr1").isNull() != col("target.main_attr1").isNull()),
        concat(
            lit("main_attr1: source="), col("source.main_attr1"),
            lit(", target="), col("target.main_attr1")
        )
    ).alias("main_attr1_diff"),
    when(
        (col("source.main_attr2") != col("target.main_attr2")) |
        (col("source.main_attr2").isNull() != col("target.main_attr2").isNull()),
        concat(
            lit("main_attr2: source="), col("source.main_attr2"),
            lit(", target="), col("target.main_attr2")
        )
    ).alias("main_attr2_diff")
).filter("main_attr1_diff IS NOT NULL OR main_attr2_diff IS NOT NULL")

print("=== Main Level Differences ===")
main_differences.show(truncate=False)

# Reconciliation of Nested Entities
# Perform a full outer join on nested_id for nested entity reconciliation
joined_nested = source_flat.alias("source").join(
    target_flat.alias("target"),
    on="nested_id",
    how="full_outer"
)

# Compare nested attributes
nested_differences = joined_nested.select(
    "nested_id",
    when(
        (col("source.nested_attr1") != col("target.nested_attr1")) |
        (col("source.nested_attr1").isNull() != col("target.nested_attr1").isNull()),
        concat(
            lit("nested_attr1: source="), col("source.nested_attr1"),
            lit(", target="), col("target.nested_attr1")
        )
    ).alias("nested_attr1_diff"),
    when(
        (col("source.nested_attr2") != col("target.nested_attr2")) |
        (col("source.nested_attr2").isNull() != col("target.nested_attr2").isNull()),
        concat(
            lit("nested_attr2: source="), col("source.nested_attr2"),
            lit(", target="), col("target.nested_attr2")
        )
    ).alias("nested_attr2_diff")
).filter("nested_attr1_diff IS NOT NULL OR nested_attr2_diff IS NOT NULL")

print("=== Nested Level Differences ===")
nested_differences.show(truncate=False)

# Aggregating and Reporting Differences
# Consolidate main differences
main_consolidated = main_differences.withColumn(
    "main_diffs",
    concat_ws(", ", "main_attr1_diff", "main_attr2_diff")
).select("main_id", "main_diffs")

# Consolidate nested differences
nested_consolidated = nested_differences.withColumn(
    "nested_diffs",
    concat_ws(", ", "nested_attr1_diff", "nested_attr2_diff")
).select("nested_id", "nested_diffs")

# Associate nested_id with main_id by joining with source_flat and target_flat
# This assumes that nested_id is unique across main_id
nested_to_main = source_flat.select("nested_id", "main_id").union(
    target_flat.select("nested_id", "main_id")
).distinct()

nested_report = nested_consolidated.join(
    nested_to_main,
    on="nested_id",
    how="left"
).select("main_id", "nested_id", "nested_diffs")

# Show consolidated reports
print("=== Main Level Consolidated Differences ===")
main_consolidated.show(truncate=False)

print("\n=== Nested Level Consolidated Differences ===")
nested_report.show(truncate=False)

# Optimization Techniques

# a. Broadcast Joins
# Assume target_flat is smaller and can be broadcasted
joined_nested_broadcast = source_flat.alias("source").join(
    broadcast(target_flat.alias("target")),
    on="nested_id",
    how="full_outer"
)

# Compare nested attributes after broadcast join
nested_differences_broadcast = joined_nested_broadcast.select(
    "nested_id",
    when(
        (col("source.nested_attr1") != col("target.nested_attr1")) |
        (col("source.nested_attr1").isNull() != col("target.nested_attr1").isNull()),
        concat(
            lit("nested_attr1: source="), col("source.nested_attr1"),
            lit(", target="), col("target.nested_attr1")
        )
    ).alias("nested_attr1_diff"),
    when(
        (col("source.nested_attr2") != col("target.nested_attr2")) |
        (col("source.nested_attr2").isNull() != col("target.nested_attr2").isNull()),
        concat(
            lit("nested_attr2: source="), col("source.nested_attr2"),
            lit(", target="), col("target.nested_attr2")
        )
    ).alias("nested_attr2_diff")
).filter("nested_attr1_diff IS NOT NULL OR nested_attr2_diff IS NOT NULL")

print("=== Nested Level Differences with Broadcast Join ===")
nested_differences_broadcast.show(truncate=False)

# b. Repartitioning
# Define number of partitions based on cluster resources
num_partitions = 200  # Adjust as needed

# Repartition DataFrames on nested_id
source_repart = source_flat.repartition(num_partitions, "nested_id")
target_repart = target_flat.repartition(num_partitions, "nested_id")

# Perform the join
joined_nested_repart = source_repart.alias("source").join(
    target_repart.alias("target"),
    on="nested_id",
    how="full_outer"
)

# Compare nested attributes after repartition
nested_differences_repart = joined_nested_repart.select(
    "nested_id",
    when(
        (col("source.nested_attr1") != col("target.nested_attr1")) |
        (col("source.nested_attr1").isNull() != col("target.nested_attr1").isNull()),
        concat(
            lit("nested_attr1: source="), col("source.nested_attr1"),
            lit(", target="), col("target.nested_attr1")
        )
    ).alias("nested_attr1_diff"),
    when(
        (col("source.nested_attr2") != col("target.nested_attr2")) |
        (col("source.nested_attr2").isNull() != col("target.nested_attr2").isNull()),
        concat(
            lit("nested_attr2: source="), col("source.nested_attr2"),
            lit(", target="), col("target.nested_attr2")
        )
    ).alias("nested_attr2_diff")
).filter("nested_attr1_diff IS NOT NULL OR nested_attr2_diff IS NOT NULL")

print("=== Nested Level Differences with Repartitioning ===")
nested_differences_repart.show(truncate=False)

# c. Caching Intermediate Results
# Cache the flattened DataFrames if used multiple times
source_flat.cache()
target_flat.cache()

# d. Filtering Irrelevant Data Early
# Example: Exclude inactive main entities
source_filtered = source_flat.filter(col("main_attr2") != "Inactive")
target_filtered = target_flat.filter(col("main_attr2") != "Inactive")

# Proceed with reconciliation using filtered DataFrames
# Re-define joined_nested with filtered DataFrames
joined_nested_filtered = source_filtered.alias("source").join(
    target_filtered.alias("target"),
    on="nested_id",
    how="full_outer"
)

# Compare nested attributes after filtering
nested_differences_filtered = joined_nested_filtered.select(
    "nested_id",
    when(
        (col("source.nested_attr1") != col("target.nested_attr1")) |
        (col("source.nested_attr1").isNull() != col("target.nested_attr1").isNull()),
        concat(
            lit("nested_attr1: source="), col("source.nested_attr1"),
            lit(", target="), col("target.nested_attr1")
        )
    ).alias("nested_attr1_diff"),
    when(
        (col("source.nested_attr2") != col("target.nested_attr2")) |
        (col("source.nested_attr2").isNull() != col("target.nested_attr2").isNull()),
        concat(
            lit("nested_attr2: source="), col("source.nested_attr2"),
            lit(", target="), col("target.nested_attr2")
        )
    ).alias("nested_attr2_diff")
).filter("nested_attr1_diff IS NOT NULL OR nested_attr2_diff IS NOT NULL")

print("=== Nested Level Differences after Filtering ===")
nested_differences_filtered.show(truncate=False)

# e. Ensuring Schema Consistency
# Example: Cast nested_attr1 to string in both DataFrames
source_flat = source_flat.withColumn("nested_attr1", col("nested_attr1").cast("string"))
target_flat = target_flat.withColumn("nested_attr1", col("nested_attr1").cast("string"))
