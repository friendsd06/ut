# Import necessary libraries
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import explode, col, when, concat, lit, concat_ws, broadcast, array
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, ArrayType

# ----------------------------
# 1. Initialize SparkSession
# ----------------------------

def initialize_spark(app_name="GenericNestedDataFrameReconciliation"):
    """
    Initializes and returns a SparkSession.
    """
    spark = SparkSession.builder \
        .appName(app_name) \
        .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    return spark

# ----------------------------
# 2. Define Sample DataFrames
# ----------------------------

def create_sample_dataframes(spark: SparkSession):
    """
    Creates sample source and target DataFrames for demonstration.
    Replace this function's content with actual data loading logic.
    """
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
            {"nested_id": 101, "nested_attr1": "NA1", "nested_attr2": "NY1"},
            {"nested_id": 102, "nested_attr1": "NA2_modified", "nested_attr2": "NY2"}
        ]),
        (2, "MainB", "Active", [
            {"nested_id": 103, "nested_attr1": "NB1", "nested_attr2": "NY3"},
            {"nested_id": 105, "nested_attr1": "NB2", "nested_attr2": "NY5"}
        ]),
        (4, "MainD", "Active", [
            {"nested_id": 106, "nested_attr1": "ND1", "nested_attr2": "NY6"}
        ])
    ]

    # Create DataFrames
    source_df = spark.createDataFrame(data_source, schema_source)
    target_df = spark.createDataFrame(data_target, schema_target)

    return source_df, target_df

# ----------------------------
# 3. Define Generic Flattening Function
# ----------------------------

def flatten_nested_entities(df: DataFrame, nested_col: str, nested_fields: list, parent_keys: list) -> DataFrame:
    """
    Flattens a DataFrame by exploding a nested array of structs.
    """
    df_exploded = df.select(
        *parent_keys,
        explode(col(nested_col)).alias("nested_entity")
    )

    select_expr = parent_keys.copy()
    for field in nested_fields:
        select_expr.append(col(f"nested_entity.{field}").alias(field))

    return df_exploded.select(*select_expr)

# ----------------------------
# 4. Define Generic Reconciliation Function
# ----------------------------

def reconcile_dataframes_unified_enhanced(
        source_flat: DataFrame,
        target_flat: DataFrame,
        parent_primary_key: str,
        child_primary_key: str,
        main_compare_cols: list,
        nested_compare_cols: list
) -> DataFrame:
    """
    Reconciles source and target flattened DataFrames and aggregates differences into a unified report.
    """
    # Reconcile Top-Level Entities
    joined_main = source_flat.select(parent_primary_key, *main_compare_cols).alias("source").join(
        target_flat.select(parent_primary_key, *main_compare_cols).alias("target"),
        on=parent_primary_key,
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

    main_diff_df = joined_main.select(parent_primary_key, *main_diffs).filter(
        " OR ".join([f"{col}_diff IS NOT NULL" for col in main_compare_cols])
    ).withColumn("diff_type", lit("Main"))

    # Reconcile Nested Entities
    joined_nested = source_flat.select(parent_primary_key, child_primary_key, *nested_compare_cols).alias("source").join(
        target_flat.select(parent_primary_key, child_primary_key, *nested_compare_cols).alias("target"),
        on=[parent_primary_key, child_primary_key],
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

    nested_diff_df = joined_nested.select(
        parent_primary_key,
        child_primary_key,
        *nested_diffs
    ).filter(
        " OR ".join([f"{col}_diff IS NOT NULL" for col in nested_compare_cols])
    ).withColumn("diff_type", lit("Nested"))

    # Combine nested diffs into a single column
    nested_diff_df = nested_diff_df.withColumn(
        "nested_diffs",
        concat_ws(", ", *[col(f"{col}_diff") for col in nested_compare_cols])
    )

    # Prepare main diffs
    main_diff_prepared = main_diff_df.select(
        parent_primary_key,
        "diff_type",
        concat_ws(", ", *[col(f"{c}_diff") for c in main_compare_cols]).alias("main_diffs"),
        lit(None).alias("nested_id"),
        lit(None).alias("nested_diffs")
    )

    # Prepare nested diffs
    nested_diff_prepared = nested_diff_df.select(
        parent_primary_key,
        "diff_type",
        lit(None).alias("main_diffs"),
        col(child_primary_key).alias("nested_id"),
        "nested_diffs"
    )

    # Union main diffs and nested diffs
    unified_report = main_diff_prepared.unionByName(nested_diff_prepared)

    # Optional: Order the report for better readability
    return unified_report.orderBy(parent_primary_key.asc().na_last(), "nested_id".asc().na_last())

# ----------------------------
# 5. Generate Unified Report
# ----------------------------

def generate_unified_report(df: DataFrame, parent_primary_key: str) -> DataFrame:
    """
    Generates a user-friendly unified reconciliation report.
    """
    return df.select(
        parent_primary_key,
        "diff_type",
        "main_diffs",
        "nested_id",
        "nested_diffs"
    )

# ----------------------------
# 6. Main Execution Function
# ----------------------------

def main():
    # Initialize Spark
    spark = initialize_spark()

    # Create or load DataFrames
    source_df, target_df = create_sample_dataframes(spark)

    # Define primary keys and nested column
    parent_primary_key = "main_id"
    child_primary_key = "nested_id"
    nested_column = "nested_entities"

    # Define columns to compare at main and nested levels
    main_columns = [field.name for field in source_df.schema.fields
                    if field.name != parent_primary_key and field.name != nested_column]
    nested_fields = [field.name for field in source_df.schema[nested_column].dataType.elementType.fields]
    nested_compare_cols = [field for field in nested_fields if field != child_primary_key]

    # Flatten both DataFrames
    source_flat = flatten_nested_entities(source_df, nested_column, nested_fields, [parent_primary_key])
    target_flat = flatten_nested_entities(target_df, nested_column, nested_fields, [parent_primary_key])

    # Display flattened DataFrames
    print("=== Source Flattened DataFrame ===")
    source_flat.show(truncate=False)

    print("\n=== Target Flattened DataFrame ===")
    target_flat.show(truncate=False)

    # Perform reconciliation
    reconciliation_report = reconcile_dataframes_unified_enhanced(
        source_flat=source_flat,
        target_flat=target_flat,
        parent_primary_key=parent_primary_key,
        child_primary_key=child_primary_key,
        main_compare_cols=main_columns,
        nested_compare_cols=nested_compare_cols
    )

    print("\n=== Unified Reconciliation Report ===")
    reconciliation_report.show(truncate=False)

    # Generate formatted unified report
    formatted_unified_report = generate_unified_report(reconciliation_report, parent_primary_key)

    print("\n=== Formatted Unified Reconciliation Report ===")
    formatted_unified_report.show(truncate=False)

    # Stop SparkSession
    spark.stop()

# ----------------------------
# 7. Execute Main Function
# ----------------------------

if __name__ == "__main__":
    main()