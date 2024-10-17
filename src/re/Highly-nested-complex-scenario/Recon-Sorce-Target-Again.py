from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import explode, col, when, concat, lit, concat_ws, array, struct
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, ArrayType

def initialize_spark(app_name="GenericNestedDataFrameReconciliation"):
    spark = SparkSession.builder.appName(app_name).getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    return spark

def create_sample_dataframes(spark: SparkSession):
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
    exploded = df.select(*parent_keys, explode(col(nested_col)).alias("nested_entity"))

    select_expr = parent_keys.copy()
    for field in nested_fields:
        select_expr.append(col(f"nested_entity.{field}").alias(field))

    return exploded.select(*select_expr)

def compare_columns(source_col, target_col, col_name):
    return when(
        (source_col != target_col) |
        (source_col.isNull() & target_col.isNotNull()) |
        (source_col.isNotNull() & target_col.isNull()),
        concat(lit(f"{col_name}: "),
               when(source_col.isNotNull(), concat(lit("source="), source_col)).otherwise(lit("source=NULL")),
               lit(", "),
               when(target_col.isNotNull(), concat(lit("target="), target_col)).otherwise(lit("target=NULL")))
    ).otherwise(None)

def reconcile_dataframes(
        source_flat: DataFrame,
        target_flat: DataFrame,
        parent_primary_key: str,
        child_primary_key: str,
        main_compare_cols: list,
        nested_compare_cols: list
) -> DataFrame:
    # Reconcile main-level entities
    joined_main = source_flat.select(parent_primary_key, *main_compare_cols).distinct() \
        .join(target_flat.select(parent_primary_key, *main_compare_cols).distinct(),
              on=parent_primary_key, how="full_outer")

    main_diffs = [compare_columns(col(f"source.{c}"), col(f"target.{c}"), c).alias(f"{c}_diff")
                  for c in main_compare_cols]

    main_diff_df = joined_main.select(
        parent_primary_key,
        *main_diffs,
        lit("Main").alias("diff_type")
    ).filter(" OR ".join([f"{c}_diff IS NOT NULL" for c in main_compare_cols]))

    # Reconcile nested entities
    joined_nested = source_flat.select(parent_primary_key, child_primary_key, *nested_compare_cols) \
        .join(target_flat.select(parent_primary_key, child_primary_key, *nested_compare_cols),
              on=[parent_primary_key, child_primary_key], how="full_outer")

    nested_diffs = [compare_columns(col(f"source.{c}"), col(f"target.{c}"), c).alias(f"{c}_diff")
                    for c in nested_compare_cols]

    nested_diff_df = joined_nested.select(
        parent_primary_key,
        child_primary_key,
        *nested_diffs,
        lit("Nested").alias("diff_type")
    ).filter(" OR ".join([f"{c}_diff IS NOT NULL" for c in nested_compare_cols]))

    # Combine main and nested diffs
    main_diff_prepared = main_diff_df.select(
        col(parent_primary_key),
        "diff_type",
        concat_ws(", ", *[col(f"{c}_diff") for c in main_compare_cols]).alias("diffs"),
        lit(None).alias(child_primary_key)
    )

    nested_diff_prepared = nested_diff_df.select(
        col(parent_primary_key),
        "diff_type",
        concat_ws(", ", *[col(f"{c}_diff") for c in nested_compare_cols]).alias("diffs"),
        col(child_primary_key)
    )

    unified_report = main_diff_prepared.unionByName(nested_diff_prepared)
    return unified_report.orderBy(col(parent_primary_key).asc(), col(child_primary_key).asc().nullsFirst())

def main():
    spark = initialize_spark()
    source_df, target_df = create_sample_dataframes(spark)

    parent_primary_key = "main_id"
    child_primary_key = "nested_id"
    nested_column = "nested_entities"

    main_columns = [f.name for f in source_df.schema.fields
                    if f.name not in {parent_primary_key, nested_column}]
    nested_fields = [f.name for f in source_df.schema[nested_column].dataType.elementType.fields]
    nested_compare_cols = [f for f in nested_fields if f != child_primary_key]

    source_flat = flatten_nested_entities(source_df, nested_column, nested_fields, [parent_primary_key])
    target_flat = flatten_nested_entities(target_df, nested_column, nested_fields, [parent_primary_key])

    print("=== Source Flattened DataFrame ===")
    source_flat.show(truncate=False)

    print("\n=== Target Flattened DataFrame ===")
    target_flat.show(truncate=False)

    reconciliation_report = reconcile_dataframes(
        source_flat=source_flat,
        target_flat=target_flat,
        parent_primary_key=parent_primary_key,
        child_primary_key=child_primary_key,
        main_compare_cols=main_columns,
        nested_compare_cols=nested_compare_cols
    )

    print("\n=== Unified Reconciliation Report ===")
    reconciliation_report.show(truncate=False)

    #spark.stop()

if __name__ == "__main__":
    main()