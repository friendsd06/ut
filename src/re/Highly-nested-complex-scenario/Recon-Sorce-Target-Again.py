from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import explode, col, when, concat, lit, concat_ws
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, ArrayType

def initialize_spark(app_name="GenericNestedDataFrameReconciliation"):
    """
    Initialize a Spark session with the specified application name.
    """
    spark = SparkSession.builder.appName(app_name).getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    return spark

def create_sample_dataframes(spark: SparkSession):
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
        (3, "MainC", "Inactive", [
            {"nested_id": 104, "nested_attr1": "NC1", "nested_attr2": "NY4"}
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
        ])
    ]

    source_df = spark.createDataFrame(data_source, schema)
    target_df = spark.createDataFrame(data_target, schema)

    return source_df, target_df

def flatten_nested_entities(df: DataFrame, nested_col: str, nested_fields: list, parent_keys: list) -> DataFrame:
    """
    Flatten nested entities by exploding the nested column and selecting relevant fields.
    """
    exploded = df.select(*parent_keys, explode(col(nested_col)).alias("nested_entity"))

    select_expr = parent_keys.copy()
    for field in nested_fields:
        select_expr.append(col(f"nested_entity.{field}").alias(field))

    return exploded.select(*select_expr)

def compare_columns(source_col: 'Column', target_col: 'Column', col_name: str):
    """
    Compare two columns and return a formatted difference string if they differ.
    """
    return when(
        (source_col != target_col) |
        (source_col.isNull() & target_col.isNotNull()) |
        (source_col.isNotNull() & target_col.isNull()),
        concat(
            lit(f"{col_name}: "),
            when(source_col.isNotNull(), concat(lit("source="), source_col)).otherwise(lit("source=NULL")),
            lit(", "),
            when(target_col.isNotNull(), concat(lit("target="), target_col)).otherwise(lit("target=NULL"))
        )
    ).otherwise(None)

def reconcile_main_entities(
        source_df: DataFrame,
        target_df: DataFrame,
        parent_primary_key: str,
        main_compare_cols: list
) -> DataFrame:
    """
    Reconcile main-level entities between source and target DataFrames.
    """
    # Prefix columns to differentiate source and target
    source_prefixed = source_df.select(
        col(parent_primary_key).alias("source_" + parent_primary_key),
        *[col(c).alias(f"source_{c}") for c in main_compare_cols]
    )

    target_prefixed = target_df.select(
        col(parent_primary_key).alias("target_" + parent_primary_key),
        *[col(c).alias(f"target_{c}") for c in main_compare_cols]
    )

    # Perform full outer join on main_id
    joined = source_prefixed.join(
        target_prefixed,
        source_prefixed["source_" + parent_primary_key] == target_prefixed["target_" + parent_primary_key],
        how="full_outer"
    ).select(
        when(col("source_" + parent_primary_key).isNotNull(), col("source_" + parent_primary_key))
            .otherwise(col("target_" + parent_primary_key))
            .alias(parent_primary_key),
        *main_compare_cols
    )

    # Add comparison columns
    diff_cols = [
        compare_columns(col(f"source_{c}"), col(f"target_{c}"), c).alias(f"{c}_diff")
        for c in main_compare_cols
    ]

    diff_df = joined.select(
        col(parent_primary_key),
        *diff_cols,
        lit("Main").alias("diff_type")
    ).filter(
        " OR ".join([f"{c}_diff IS NOT NULL" for c in main_compare_cols])
    )

    # Concatenate differences into a single string
    diff_df = diff_df.select(
        col(parent_primary_key),
        "diff_type",
        concat_ws(", ", *[col(f"{c}_diff") for c in main_compare_cols]).alias("diffs"),
        lit(None).alias("nested_id")  # Placeholder for nested_id
    )

    return diff_df

def reconcile_nested_entities(
        source_flat: DataFrame,
        target_flat: DataFrame,
        parent_primary_key: str,
        child_primary_key: str,
        nested_compare_cols: list
) -> DataFrame:
    """
    Reconcile nested-level entities between flattened source and target DataFrames.
    """
    # Prefix columns to differentiate source and target
    source_prefixed = source_flat.select(
        col(parent_primary_key).alias("source_" + parent_primary_key),
        col(child_primary_key).alias("source_" + child_primary_key),
        *[col(c).alias(f"source_{c}") for c in nested_compare_cols]
    )

    target_prefixed = target_flat.select(
        col(parent_primary_key).alias("target_" + parent_primary_key),
        col(child_primary_key).alias("target_" + child_primary_key),
        *[col(c).alias(f"target_{c}") for c in nested_compare_cols]
    )

    # Perform full outer join on main_id and nested_id
    join_conditions = [
        source_prefixed["source_" + parent_primary_key] == target_prefixed["target_" + parent_primary_key],
        source_prefixed["source_" + child_primary_key] == target_prefixed["target_" + child_primary_key]
    ]

    joined = source_prefixed.join(
        target_prefixed,
        on=join_conditions,
        how="full_outer"
    ).select(
        when(col("source_" + parent_primary_key).isNotNull(), col("source_" + parent_primary_key))
            .otherwise(col("target_" + parent_primary_key))
            .alias(parent_primary_key),
        when(col("source_" + child_primary_key).isNotNull(), col("source_" + child_primary_key))
            .otherwise(col("target_" + child_primary_key))
            .alias(child_primary_key),
        *nested_compare_cols
    )

    # Add comparison columns
    diff_cols = [
        compare_columns(col(f"source_{c}"), col(f"target_{c}"), c).alias(f"{c}_diff")
        for c in nested_compare_cols
    ]

    diff_df = joined.select(
        col(parent_primary_key),
        col(child_primary_key),
        *diff_cols,
        lit("Nested").alias("diff_type")
    ).filter(
        " OR ".join([f"{c}_diff IS NOT NULL" for c in nested_compare_cols])
    )

    # Concatenate differences into a single string
    diff_df = diff_df.select(
        col(parent_primary_key),
        "diff_type",
        concat_ws(", ", *[col(f"{c}_diff") for c in nested_compare_cols]).alias("diffs"),
        col(child_primary_key)
    )

    return diff_df

def reconcile_dataframes(
        source_df: DataFrame,
        target_df: DataFrame,
        parent_primary_key: str,
        child_primary_key: str,
        main_compare_cols: list,
        nested_compare_cols: list
) -> DataFrame:
    """
    Reconcile source and target DataFrames by comparing main and nested entities.
    """
    # Reconcile main-level entities
    main_diff_df = reconcile_main_entities(
        source_df=source_df,
        target_df=target_df,
        parent_primary_key=parent_primary_key,
        main_compare_cols=main_compare_cols
    )

    # Flatten nested entities
    source_flat = flatten_nested_entities(source_df, "nested_entities", nested_compare_cols, [parent_primary_key])
    target_flat = flatten_nested_entities(target_df, "nested_entities", nested_compare_cols, [parent_primary_key])

    # Reconcile nested-level entities
    nested_diff_df = reconcile_nested_entities(
        source_flat=source_flat,
        target_flat=target_flat,
        parent_primary_key=parent_primary_key,
        child_primary_key=child_primary_key,
        nested_compare_cols=nested_compare_cols
    )

    # Combine main and nested diffs
    unified_report = main_diff_df.unionByName(nested_diff_df)

    return unified_report.orderBy(col(parent_primary_key).asc(), col(child_primary_key).asc().nullsFirst())

def main():
    """
    Main function to execute the reconciliation process.
    """
    spark = initialize_spark()
    source_df, target_df = create_sample_dataframes(spark)

    parent_primary_key = "main_id"
    child_primary_key = "nested_id"

    # Identify main-level columns excluding primary key and nested column
    main_columns = [f.name for f in source_df.schema.fields
                    if f.name not in {parent_primary_key, "nested_entities"}]

    # Identify nested-level fields
    nested_fields = [f.name for f in source_df.schema["nested_entities"].dataType.elementType.fields]

    # Columns to compare in nested entities (excluding child primary key)
    nested_compare_cols = [f for f in nested_fields if f != child_primary_key]

    # Generate reconciliation report
    reconciliation_report = reconcile_dataframes(
        source_df=source_df,
        target_df=target_df,
        parent_primary_key=parent_primary_key,
        child_primary_key=child_primary_key,
        main_compare_cols=main_columns,
        nested_compare_cols=nested_compare_cols
    )

    # Display results
    print("=== Source DataFrame ===")
    source_df.show(truncate=False)

    print("\n=== Target DataFrame ===")
    target_df.show(truncate=False)

    print("\n=== Unified Reconciliation Report ===")
    reconciliation_report.show(truncate=False)

    spark.stop()

if __name__ == "__main__":
    main()
