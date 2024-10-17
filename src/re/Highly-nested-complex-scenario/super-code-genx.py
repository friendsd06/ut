from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    explode_outer,
    col,
    when,
    concat,
    lit,
    concat_ws,
    coalesce
)
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    ArrayType
)
from functools import reduce

def initialize_spark(app_name: str = "NestedDataFrameReconciliation") -> SparkSession:
    """
    Initialize a Spark session with the specified application name.
    """
    spark = SparkSession.builder.appName(app_name).getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    return spark

def create_sample_dataframes(spark: SparkSession) -> (DataFrame, DataFrame):
    """
    Create sample source and target DataFrames with nested structures.
    """
    schema = StructType([
        StructField("main_id", IntegerType(), True),
        StructField("main_attr1", StringType(), True),
        StructField("main_attr2", StringType(), True),
        StructField("nested_entities", ArrayType(StructType([
            StructField("nested_id", IntegerType(), True),
            StructField("nested_attr1", StringType(), True),
            StructField("nested_attr2", StringType(), True)
        ])), True)
    ])

    data_source = [
        (1, "MainA", "Active", [
            {"nested_id": 101, "nested_attr1": "NA1", "nested_attr2": "NY1"},
            {"nested_id": 102, "nested_attr1": "NA2", "nested_attr2": "NY2"}
        ]),
        (2, "MainB", "Active", [
            {"nested_id": 103, "nested_attr1": "NB1", "nested_attr2": "NY3"}
        ]),
        (3, "MainC", "Inactive", None),  # nested_entities is None
        (5, None, "Active", [  # main_attr1 is None
            {"nested_id": 107, "nested_attr1": None, "nested_attr2": "NY7"}  # nested_attr1 is None
        ])
    ]

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
        ]),
        (5, "MainE", "Inactive", None)  # nested_entities is None
    ]

    source_df = spark.createDataFrame(data_source, schema)
    target_df = spark.createDataFrame(data_target, schema)

    return source_df, target_df

def flatten_nested_entities(
        df: DataFrame,
        nested_col: str,
        nested_fields: list,
        parent_keys: list
) -> DataFrame:
    """
    Flatten nested entities by exploding the nested column and selecting relevant fields.
    """
    exploded = df.select(
        *parent_keys,
        explode_outer(col(nested_col)).alias("nested_entity")
    )

    select_expr = parent_keys.copy()
    for field in nested_fields:
        select_expr.append(col(f"nested_entity.{field}").alias(field))

    return exploded.select(*select_expr)

def compare_columns(
        source_col: 'Column',
        target_col: 'Column',
        col_name: str
) -> 'Column':
    """
    Compare two columns and return a formatted difference string if they differ.
    """
    return when(
        (source_col != target_col) |
        (source_col.isNull() & target_col.isNotNull()) |
        (source_col.isNotNull() & target_col.isNull()),
        concat(
            lit(f"{col_name}: "),
            when(source_col.isNotNull(), concat(lit("source="), source_col.cast(StringType()))).otherwise(lit("source=NULL")),
            lit(", "),
            when(target_col.isNotNull(), concat(lit("target="), target_col.cast(StringType()))).otherwise(lit("target=NULL"))
        )
    )

def reconcile_dataframes(
        source_flat: DataFrame,
        target_flat: DataFrame,
        parent_primary_key: str,
        child_primary_key: str,
        main_compare_cols: list,
        nested_compare_cols: list
) -> DataFrame:
    """
    Reconcile source and target DataFrames by comparing main and nested entities.
    """
    # ------------------------
    # Reconcile Main-Level Entities
    # ------------------------

    # Select and alias main-level columns
    source_main = source_flat.select(
        col(parent_primary_key).alias("source_" + parent_primary_key),
        *[col(c).alias(f"source_{c}") for c in main_compare_cols]
    ).distinct()

    target_main = target_flat.select(
        col(parent_primary_key).alias("target_" + parent_primary_key),
        *[col(c).alias(f"target_{c}") for c in main_compare_cols]
    ).distinct()

    # Perform full outer join on parent_primary_key
    joined_main = source_main.join(
        target_main,
        source_main["source_" + parent_primary_key] == target_main["target_" + parent_primary_key],
        how="full_outer"
    ).select(
        coalesce(source_main["source_" + parent_primary_key], target_main["target_" + parent_primary_key]).alias(parent_primary_key),
        *[source_main["source_" + c] for c in main_compare_cols],
        *[target_main["target_" + c] for c in main_compare_cols]
    )

    # Compare main-level columns
    main_diffs = [
        compare_columns(col(f"source_{c}"), col(f"target_{c}"), c).alias(f"{c}_diff")
        for c in main_compare_cols
    ]

    main_diff_df = joined_main.select(
        col(parent_primary_key),
        *main_diffs,
        lit("Main").alias("diff_type")
    )

    # Build filter condition
    main_conditions = [col(f"{c}_diff").isNotNull() for c in main_compare_cols]
    if main_conditions:
        main_combined_condition = reduce(lambda a, b: a | b, main_conditions)
        main_diff_df = main_diff_df.filter(main_combined_condition)
    else:
        main_diff_df = main_diff_df.filter(lit(False))  # No columns to compare

    # Concatenate differences into a single string
    main_diff_prepared = main_diff_df.select(
        col(parent_primary_key),
        "diff_type",
        concat_ws(", ", *[col(f"{c}_diff") for c in main_compare_cols]).alias("diffs"),
        lit(None).cast(StringType()).alias(child_primary_key)  # Placeholder for nested_id
    )

    # ------------------------
    # Reconcile Nested-Level Entities
    # ------------------------

    # Select and alias nested-level columns
    source_nested = source_flat.select(
        col(parent_primary_key).alias("source_" + parent_primary_key),
        col(child_primary_key).alias("source_" + child_primary_key),
        *[col(c).alias(f"source_{c}") for c in nested_compare_cols]
    )

    target_nested = target_flat.select(
        col(parent_primary_key).alias("target_" + parent_primary_key),
        col(child_primary_key).alias("target_" + child_primary_key),
        *[col(c).alias(f"target_{c}") for c in nested_compare_cols]
    )

    # Perform full outer join on parent_primary_key and child_primary_key
    joined_nested = source_nested.join(
        target_nested,
        on=[
            source_nested["source_" + parent_primary_key] == target_nested["target_" + parent_primary_key],
            source_nested["source_" + child_primary_key] == target_nested["target_" + child_primary_key]
        ],
        how="full_outer"
    ).select(
        coalesce(source_nested["source_" + parent_primary_key], target_nested["target_" + parent_primary_key]).alias(parent_primary_key),
        coalesce(source_nested["source_" + child_primary_key], target_nested["target_" + child_primary_key]).alias(child_primary_key),
        *[source_nested[f"source_{c}"] for c in nested_compare_cols],
        *[target_nested[f"target_{c}"] for c in nested_compare_cols]
    )

    # Compare nested-level columns
    nested_diffs = [
        compare_columns(col(f"source_{c}"), col(f"target_{c}"), c).alias(f"{c}_diff")
        for c in nested_compare_cols
    ]

    nested_diff_df = joined_nested.select(
        col(parent_primary_key),
        col(child_primary_key),
        *nested_diffs,
        lit("Nested").alias("diff_type")
    )

    # Build filter condition
    nested_conditions = [col(f"{c}_diff").isNotNull() for c in nested_compare_cols]
    if nested_conditions:
        nested_combined_condition = reduce(lambda a, b: a | b, nested_conditions)
        nested_diff_df = nested_diff_df.filter(nested_combined_condition)
    else:
        nested_diff_df = nested_diff_df.filter(lit(False))  # No columns to compare

    # Concatenate differences into a single string
    nested_diff_prepared = nested_diff_df.select(
        col(parent_primary_key),
        "diff_type",
        concat_ws(", ", *[col(f"{c}_diff") for c in nested_compare_cols]).alias("diffs"),
        col(child_primary_key)
    )

    # ------------------------
    # Combine Main and Nested Diffs
    # ------------------------

    unified_report = main_diff_prepared.unionByName(nested_diff_prepared)

    # Order the report for readability
    return unified_report.orderBy(
        col(parent_primary_key).asc(),
        col(child_primary_key).asc().nullsFirst()
    )

def main():
    """
    Main function to execute the reconciliation process.
    """
    # Initialize Spark session
    spark = initialize_spark()

    # Create sample DataFrames
    source_df, target_df = create_sample_dataframes(spark)

    # Define primary keys and nested column
    parent_primary_key = "main_id"
    child_primary_key = "nested_id"
    nested_column = "nested_entities"

    # Identify main-level columns excluding primary key and nested column
    main_compare_cols = [
        field.name for field in source_df.schema.fields
        if field.name not in {parent_primary_key, nested_column}
    ]

    # Identify nested-level fields
    nested_fields = [
        field.name for field in source_df.schema[nested_column].dataType.elementType.fields
    ]

    # Columns to compare in nested entities (excluding child primary key)
    nested_compare_cols = [
        field for field in nested_fields if field != child_primary_key
    ]

    # Flatten nested entities in source and target DataFrames
    source_flat = flatten_nested_entities(
        df=source_df,
        nested_col=nested_column,
        nested_fields=nested_fields,
        parent_keys=[parent_primary_key]
    )

    target_flat = flatten_nested_entities(
        df=target_df,
        nested_col=nested_column,
        nested_fields=nested_fields,
        parent_keys=[parent_primary_key]
    )

    # Display Flattened DataFrames
    print("=== Source Flattened DataFrame ===")
    source_flat.show(truncate=False)

    print("\n=== Target Flattened DataFrame ===")
    target_flat.show(truncate=False)

    # Perform reconciliation
    reconciliation_report = reconcile_dataframes(
        source_flat=source_flat,
        target_flat=target_flat,
        parent_primary_key=parent_primary_key,
        child_primary_key=child_primary_key,
        main_compare_cols=main_compare_cols,
        nested_compare_cols=nested_compare_cols
    )

    # Display Reconciliation Report
    print("\n=== Unified Reconciliation Report ===")
    reconciliation_report.show(truncate=False)

    # Stop Spark session
    spark.stop()

if __name__ == "__main__":
    main()
