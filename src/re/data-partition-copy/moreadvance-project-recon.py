from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    explode_outer,
    col,
    when,
    lit,
    coalesce
)
from pyspark.sql.types import *
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
    # Define the schema with nested StructType fields
    schema = StructType([
        StructField("main_id", IntegerType(), True),
        StructField("main_attr1", StringType(), True),
        StructField("main_attr2", StringType(), True),
        StructField("nested_entities", ArrayType(StructType([
            StructField("nested_id", IntegerType(), True),
            StructField("nested_attr1", StringType(), True),
            StructField("nested_attr2", StringType(), True),
            StructField("nested_struct", StructType([
                StructField("sub_attr1", StringType(), True),
                StructField("sub_attr2", IntegerType(), True)
            ]), True)
        ])), True)
    ])

    data_source = [
        (1, "MainA", "Active", [
            {"nested_id": 101, "nested_attr1": "NA1", "nested_attr2": "NY1", "nested_struct": {"sub_attr1": "SubA", "sub_attr2": 10}},
            {"nested_id": 102, "nested_attr1": "NA2", "nested_attr2": "NY2", "nested_struct": {"sub_attr1": "SubB", "sub_attr2": 20}}
        ]),
        (2, "MainB", "Active", [
            {"nested_id": 103, "nested_attr1": "NB1", "nested_attr2": "NY3", "nested_struct": {"sub_attr1": "SubC", "sub_attr2": 30}}
        ]),
        (3, "MainC", "Inactive", None),  # nested_entities is None
        (5, None, "Active", [
            {"nested_id": 107, "nested_attr1": None, "nested_attr2": "NY7", "nested_struct": {"sub_attr1": "SubD", "sub_attr2": 40}}
        ])
    ]

    data_target = [
        (1, "MainA", "Active", [
            {"nested_id": 101, "nested_attr1": "NA1_modified", "nested_attr2": "NY1", "nested_struct": {"sub_attr1": "SubA", "sub_attr2": 10}},
            {"nested_id": 102, "nested_attr1": "NA2", "nested_attr2": "NY2", "nested_struct": {"sub_attr1": "SubB_modified", "sub_attr2": 20}}
        ]),
        (2, "MainB", "Inactive", [
            {"nested_id": 103, "nested_attr1": "NB1", "nested_attr2": "NY3_modified", "nested_struct": {"sub_attr1": "SubC", "sub_attr2": 35}},
            {"nested_id": 105, "nested_attr1": "NB2", "nested_attr2": "NY5", "nested_struct": {"sub_attr1": "SubE", "sub_attr2": 50}}
        ]),
        (4, "MainD", "Active", [
            {"nested_id": 106, "nested_attr1": "ND1", "nested_attr2": "NY6", "nested_struct": {"sub_attr1": "SubF", "sub_attr2": 60}}
        ]),
        (5, "MainE", "Active", None)  # nested_entities is None
    ]

    source_df = spark.createDataFrame(data_source, schema)
    target_df = spark.createDataFrame(data_target, schema)

    return source_df, target_df

def flatten_struct(schema, prefix=""):
    """
    Recursively flattens a StructType schema.
    """
    fields = []
    for field in schema.fields:
        field_name = f"{prefix}{field.name}" if prefix else field.name
        if isinstance(field.dataType, StructType):
            # Flatten nested structs recursively
            fields += flatten_struct(field.dataType, prefix=f"{field_name}_")
        else:
            # Include the field with the current prefix
            fields.append(col(f"{prefix}{field.name}").alias(field_name))
    return fields

def flatten_dataframe(df, nested_column):
    """
    Flattens a DataFrame with nested StructType and ArrayType columns.
    """
    # Explode the nested array column
    df = df.withColumn(nested_column, explode_outer(col(nested_column)))
    # Get all columns except the nested array column
    non_struct_cols = [col(c) for c in df.columns if c != nested_column]
    # Flatten the nested struct columns within the nested_column without prefix
    struct_cols = []
    if nested_column in df.columns:
        # Flatten the nested_column (which is now a struct after exploding) without prefix
        nested_schema = df.schema[nested_column].dataType
        struct_cols.extend(flatten_struct(nested_schema, prefix=""))
    # Flatten other struct columns with prefixes
    for c in df.columns:
        if c != nested_column and isinstance(df.schema[c].dataType, StructType):
            struct_cols.extend(flatten_struct(df.schema[c].dataType, prefix=f"{c}_"))
    # Select all columns
    return df.select(*non_struct_cols, *struct_cols)

def reconcile_dataframes(
        source_df: DataFrame,
        target_df: DataFrame,
        primary_keys: list
) -> DataFrame:
    """
    Reconcile source and target DataFrames by comparing all columns.
    """
    # Flatten the DataFrames
    source_flat = flatten_dataframe(source_df, nested_column="nested_entities")
    target_flat = flatten_dataframe(target_df, nested_column="nested_entities")

    # Get the list of columns to compare (excluding primary keys)
    compare_cols = [c for c in source_flat.columns if c not in primary_keys]

    # Alias columns for source and target
    source_flat = source_flat.select(
        *[col(c).alias(f"source_{c}") for c in source_flat.columns]
    )
    target_flat = target_flat.select(
        *[col(c).alias(f"target_{c}") for c in target_flat.columns]
    )

    # Join on primary keys
    join_conditions = [
        source_flat[f"source_{pk}"] == target_flat[f"target_{pk}"] for pk in primary_keys
    ]
    joined_df = source_flat.join(
        target_flat,
        on=join_conditions,
        how="full_outer"
    )

    # Compare columns and collect differences
    diff_columns = []
    for c in compare_cols:
        source_col = col(f"source_{c}")
        target_col = col(f"target_{c}")
        diff_col_name = f"{c}_diff"
        diff_columns.extend([
            source_col,
            target_col,
            when(
                ~source_col.eqNullSafe(target_col),
                lit(True)
            ).otherwise(lit(False)).alias(diff_col_name)
        ])

    # Build the final DataFrame
    result_df = joined_df.select(
        *[coalesce(col(f"source_{pk}"), col(f"target_{pk}")).alias(pk) for pk in primary_keys],
        *diff_columns
    )

    # Filter rows with any differences
    diff_cond = reduce(lambda a, b: a | b, [col(f"{c}_diff") for c in compare_cols])
    result_df = result_df.filter(diff_cond)

    return result_df

def main():
    """
    Main function to execute the reconciliation process.
    """
    # Initialize Spark session
    spark = initialize_spark()

    # Create sample DataFrames with nested structures
    source_df, target_df = create_sample_dataframes(spark)

    # Define primary keys
    primary_keys = ['main_id', 'nested_id']

    # Perform reconciliation
    reconciliation_report = reconcile_dataframes(
        source_df=source_df,
        target_df=target_df,
        primary_keys=primary_keys
    )

    # Display Reconciliation Report
    print("\n=== Reconciliation Report ===")
    reconciliation_report.show(truncate=False)

    # Stop Spark session
    spark.stop()

if __name__ == "__main__":
    main()
