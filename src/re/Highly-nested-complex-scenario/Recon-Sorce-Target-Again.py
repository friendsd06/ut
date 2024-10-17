# Import necessary libraries
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import explode, col, when, concat, lit, concat_ws, broadcast
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, ArrayType

# ----------------------------
# 1. Initialize SparkSession
# ----------------------------

# Initialize SparkSession
spark = SparkSession.builder \
    .appName("GenericNestedDataFrameReconciliation") \
    .getOrCreate()

# Set log level to ERROR to reduce verbosity
spark.sparkContext.setLogLevel("ERROR")

# ----------------------------
# 2. Create Sample DataFrames
# ----------------------------

# For demonstration, we'll create sample source and target DataFrames.
# In practice, replace this section with code to read your actual data.

# Define schema for source DataFrame
schema_source = StructType([
    StructField("main_id", IntegerType(), True),            # Parent Primary Key
    StructField("main_attr1", StringType(), True),
    StructField("main_attr2", StringType(), True),
    StructField("nested_entities", ArrayType(StructType([  # Nested Entities
        StructField("nested_id", IntegerType(), True),    # Child Primary Key
        StructField("nested_attr1", StringType(), True),
        StructField("nested_attr2", StringType(), True)
    ])), True)
])

# Define schema for target DataFrame
schema_target = StructType([
    StructField("main_id", IntegerType(), True),            # Parent Primary Key
    StructField("main_attr1", StringType(), True),
    StructField("main_attr2", StringType(), True),
    StructField("nested_entities", ArrayType(StructType([  # Nested Entities
        StructField("nested_id", IntegerType(), True),    # Child Primary Key
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

# ----------------------------
# 3. Define Generic Flattening and Reconciliation Functions
# ----------------------------

def flatten_nested_entities(df: DataFrame, nested_col: str, nested_fields: list) -> DataFrame:
    """
    Flattens a DataFrame by exploding a nested array of structs.

    Args:
        df (DataFrame): The input DataFrame with nested structures.
        nested_col (str): The name of the nested column to explode.
        nested_fields (list): List of field names inside the nested struct to extract.

    Returns:
        DataFrame: A flattened DataFrame with one row per nested entity.
    """
    # Explode the nested column
    df_exploded = df.select(
        "main_id",
        "main_attr1",
        "main_attr2",
        explode(col(nested_col)).alias("nested_entity")
    )

    # Select main attributes and nested attributes
    select_expr = ["main_id", "main_attr1", "main_attr2"]
    for field in nested_fields:
        select_expr.append(col(f"nested_entity.{field}").alias(field))

    return df_exploded.select(*select_expr)

def reconcile_dataframes_unified_enhanced(
        source_flat: DataFrame,
        target_flat: DataFrame,
        main_join_key: str,
        nested_join_key: str,
        main_compare_cols: list,
        nested_compare_cols: list
) -> DataFrame:
    """
    Reconciles source and target flattened DataFrames and aggregates differences into a unified report.

    Args:
        source_flat (DataFrame): Flattened source DataFrame.
        target_flat (DataFrame): Flattened target DataFrame.
        main_join_key (str): Column name to join on for main entities.
        nested_join_key (str): Column name to join on for nested entities.
        main_compare_cols (list): List of main level columns to compare.
        nested_compare_cols (list): List of nested level columns to compare.

    Returns:
        DataFrame: Enhanced unified reconciliation report containing both main and nested differences.
    """
    # Reconcile Top-Level Entities
    joined_main = source_flat.select(main_join_key, *main_compare_cols).alias("source").join(
        target_flat.select(main_join_key, *main_compare_cols).alias("target"),
        on=main_join_key,
        how="full_outer"
    )

    # Generate differences for main level
    main_diffs = []
    for col_name in main_compare_cols:
        diff_col = when(
            (col(f"source.{col_name}") != col(f"target.{col_name}")) |
            (col(f"source.{col_name}").isNull() != col(f"target.{col_name}").isNull()),
            concat(
                lit(f"{col_name}: source="), col(f"source.{col_name}"),
                lit(", target="), col(f"target.{col_name}")
            )
        )
        main_diffs.append(diff_col.alias(f"{col_name}_diff"))

    main_diff_df = joined_main.select(main_join_key, *main_diffs).filter(
        " OR ".join([f"{col}_diff IS NOT NULL" for col in main_compare_cols])
    ).withColumn("diff_type", lit("Main"))

    # Reconcile Nested Entities
    joined_nested = source_flat.select(nested_join_key, *nested_compare_cols).alias("source").join(
        target_flat.select(nested_join_key, *nested_compare_cols).alias("target"),
        on=nested_join_key,
        how="full_outer"
    )

    # Generate differences for nested level
    nested_diffs = []
    for col_name in nested_compare_cols:
        diff_col = when(
            (col(f"source.{col_name}") != col(f"target.{col_name}")) |
            (col(f"source.{col_name}").isNull() != col(f"target.{col_name}").isNull()),
            concat(
                lit(f"{col_name}: source="), col(f"source.{col_name}"),
                lit(", target="), col(f"target.{col_name}")
            )
        )
        nested_diffs.append(diff_col.alias(f"{col_name}_diff"))

    nested_diff_df = joined_nested.select(nested_join_key, *nested_diffs).filter(
        " OR ".join([f"{col}_diff IS NOT NULL" for col in nested_compare_cols])
    ).withColumn("diff_type", lit("Nested"))

    # Associate nested_id with main_id
    # Assuming nested_join_key is unique and maps to a single main_id
    source_main_map = source_flat.select(nested_join_key, main_join_key).distinct()
    target_main_map = target_flat.select(nested_join_key, main_join_key).distinct()
    main_map = source_main_map.union(target_main_map).distinct()

    nested_report = nested_diff_df.join(
        main_map,
        on=nested_join_key,
        how="left"
    ).withColumnRenamed(nested_join_key, "nested_id")

    # Add main_id to main_diff_df
    main_diff_with_main_id = main_diff_df.join(
        main_map,
        on=main_join_key,
        how="left"
    ).select(
        main_join_key,
        "diff_type",
        *[col(f"{c}_diff") for c in main_compare_cols],
        lit(None).alias("nested_id"),
        lit(None).alias("nested_diffs")
    )

    # Prepare nested diffs with main_id
    nested_diff_prepared = nested_report.select(
        "main_id",
        "diff_type",
        lit(None).alias("main_attr1_diff"),
        lit(None).alias("main_attr2_diff"),
        "nested_id",
        "nested_diffs"
    )

    # Union main diffs and nested diffs
    unified_report = main_diff_with_main_id.unionByName(nested_diff_prepared)

    # Optional: Order the report for better readability
    return unified_report.orderBy(main_join_key.asc().na_last(), "nested_id".asc().na_last())

# ----------------------------
# 4. Perform Reconciliation
# ----------------------------

# Define primary key columns and nested column details
parent_primary_key = "main_id"          # Primary key for parent
child_primary_key = "nested_id"         # Primary key for nested entities
nested_column = "nested_entities"       # Column containing nested entities

# Define which columns to compare at main and nested levels
# For generic purposes, we'll assume all columns except the primary keys are to be compared
# Extract column names dynamically

# Get list of main level columns to compare (excluding primary key)
main_columns = [field.name for field in source_df.schema.fields if field.name != parent_primary_key and field.name != nested_column]

# Get list of nested level columns to compare (excluding primary key)
nested_columns = [field.name for field in source_df.schema.fields if field.name == nested_column][0]
nested_fields = [field.name for field in source_df.schema.fields if field.name == nested_column][0]
# Actually, to get nested field names:
nested_fields = [field.name for field in source_df.schema[nested_column].dataType.elementType.fields]

# Alternatively, define them manually if dynamic extraction is not reliable
# nested_compare_cols = ["nested_attr1", "nested_attr2"]

# Flatten both DataFrames
source_flat = flatten_nested_entities(source_df, nested_column, nested_fields)
target_flat = flatten_nested_entities(target_df, nested_column, nested_fields)

# Display flattened DataFrames
print("=== Source Flattened DataFrame ===")
source_flat.show(truncate=False)

print("\n=== Target Flattened DataFrame ===")
target_flat.show(truncate=False)

# Define columns to compare
# Exclude primary keys
main_compare_cols = [col for col in main_columns]
nested_compare_cols = [col for col in nested_fields if col != child_primary_key]

# Perform unified reconciliation
unified_reconciliation_report = reconcile_dataframes_unified_enhanced(
    source_flat=source_flat,
    target_flat=target_flat,
    main_join_key=parent_primary_key,
    nested_join_key=child_primary_key,
    main_compare_cols=main_compare_cols,
    nested_compare_cols=nested_compare_cols
)

print("=== Unified Reconciliation Report ===")
unified_reconciliation_report.show(truncate=False)

# ----------------------------
# 5. Aggregate Differences into a Unified Report
# ----------------------------

# The unified_reconciliation_report already consolidates both main and nested differences.
# However, to present it in a more user-friendly format, we can further process it.

def generate_unified_report(df: DataFrame) -> DataFrame:
    """
    Generates a user-friendly unified reconciliation report.

    Args:
        df (DataFrame): The reconciliation report DataFrame.

    Returns:
        DataFrame: Formatted unified reconciliation report.
    """
    from pyspark.sql.functions import concat_ws

    return df.select(
        parent_primary_key,
        "diff_type",
        *[col(f"{c}_diff") for c in main_compare_cols],
        "nested_id",
        "nested_diffs"
    ).withColumn(
        "main_diffs",
        concat_ws(", ", [col(f"{c}_diff") for c in main_compare_cols])
    ).withColumn(
        "all_diffs",
        when(col("diff_type") == "Main", col("main_diffs"))
            .otherwise(col("nested_diffs"))
    ).select(
        parent_primary_key,
        "diff_type",
        "all_diffs",
        "nested_id"
    )

# Generate formatted unified report
formatted_unified_report = generate_unified_report(unified_reconciliation_report)

print("=== Formatted Unified Reconciliation Report ===")
formatted_unified_report.show(truncate=False)

# ----------------------------
# 6. Optimization Techniques
# ----------------------------

# Apply optimizations as needed. For example:

# a. Broadcast Joins if target_flat is small
# Uncomment the following lines if target_flat is small enough to be broadcasted

# broadcast_target_flat = broadcast(target_flat)
# unified_reconciliation_broadcast = reconcile_dataframes_unified_enhanced(
#     source_flat=source_flat,
#     target_flat=broadcast_target_flat,
#     main_join_key=parent_primary_key,
#     nested_join_key=child_primary_key,
#     main_compare_cols=main_compare_cols,
#     nested_compare_cols=nested_compare_cols
# )
# print("=== Unified Reconciliation Report with Broadcast Join ===")
# unified_reconciliation_broadcast.show(truncate=False)

# b. Repartition DataFrames on join keys for large datasets
# Adjust the number of partitions based on your cluster's resources

# num_partitions = 200
# source_repart = source_flat.repartition(num_partitions, parent_primary_key, child_primary_key)
# target_repart = target_flat.repartition(num_partitions, child_primary_key)
# unified_reconciliation_repart = reconcile_dataframes_unified_enhanced(
#     source_flat=source_repart,
#     target_flat=target_repart,
#     main_join_key=parent_primary_key,
#     nested_join_key=child_primary_key,
#     main_compare_cols=main_compare_cols,
#     nested_compare_cols=nested_compare_cols
# )
# print("=== Unified Reconciliation Report with Repartitioning ===")
# unified_reconciliation_repart.show(truncate=False)

# c. Cache DataFrames if used multiple times
# source_flat.cache()
# target_flat.cache()

# d. Filter out irrelevant data early
# For example, exclude inactive entities
# source_filtered = source_flat.filter(col("main_attr2") != "Inactive")
# target_filtered = target_flat.filter(col("main_attr2") != "Inactive")
# Perform reconciliation using filtered DataFrames as shown earlier

# ----------------------------
# 7. Final Cleanup
# ----------------------------

# Stop SparkSession after all operations are done
#spark.stop()
