from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    explode_outer,
    col,
    when,
    lit,
    coalesce,
    struct,
    array,
    create_map,
    to_json,
    sha2
)
from pyspark.sql.types import *
from functools import reduce

def initialize_spark(app_name: str = "FlexibleDataFrameReconciliation") -> SparkSession:
    """
    Initialize a Spark session with the specified application name.
    """
    spark = SparkSession.builder.appName(app_name).getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    return spark

def create_sample_dataframes(spark: SparkSession) -> (DataFrame, DataFrame):
    """
    Create sample source and target DataFrames with complex nested structures.
    """
    schema = StructType([
        StructField("id", IntegerType(), True),
        StructField("attributes", StructType([
            StructField("attr1", StringType(), True),
            StructField("attr2", StringType(), True),
            StructField("sub_attributes", ArrayType(StructType([
                StructField("sub_attr1", StringType(), True),
                StructField("sub_attr2", StructType([
                    StructField("deep_attr1", IntegerType(), True),
                    StructField("deep_attr2", StringType(), True)
                ]), True)
            ])), True)
        ]), True),
        StructField("metadata", MapType(StringType(), StringType()), True)
    ])

    data_source = [
        (1, {"attr1": "A", "attr2": "B", "sub_attributes": [
            {"sub_attr1": "SA1", "sub_attr2": {"deep_attr1": 10, "deep_attr2": "DA1"}},
            {"sub_attr1": "SA2", "sub_attr2": {"deep_attr1": 20, "deep_attr2": "DA2"}}
        ]}, {"key1": "value1", "key2": "value2"}),
        (2, {"attr1": "C", "attr2": "D", "sub_attributes": None}, None),
        (3, None, {"key3": "value3"}),
    ]

    data_target = [
        (1, {"attr1": "A", "attr2": "B_modified", "sub_attributes": [
            {"sub_attr1": "SA1", "sub_attr2": {"deep_attr1": 10, "deep_attr2": "DA1"}},
            {"sub_attr1": "SA2", "sub_attr2": {"deep_attr1": 25, "deep_attr2": "DA2_modified"}}
        ]}, {"key1": "value1_modified", "key2": "value2"}),
        (2, {"attr1": "C", "attr2": "D", "sub_attributes": None}, {"key4": "value4"}),
        (4, {"attr1": "E", "attr2": "F", "sub_attributes": []}, None),
    ]

    source_df = spark.createDataFrame(data_source, schema)
    target_df = spark.createDataFrame(data_target, schema)

    return source_df, target_df

def flatten_schema(schema, prefix=None):
    """
    Recursively flattens a nested schema.
    """
    fields = []
    for field in schema.fields:
        field_name = f"{prefix}.{field.name}" if prefix else field.name
        data_type = field.dataType
        if isinstance(data_type, StructType):
            fields += flatten_schema(data_type, prefix=field_name)
        elif isinstance(data_type, ArrayType) and isinstance(data_type.elementType, StructType):
            # Handle arrays of structs
            fields.append((field_name, data_type, 'array_struct'))
        else:
            fields.append((field_name, data_type, 'primitive'))
    return fields

def flatten_dataframe(df, prefix=None):
    """
    Recursively flattens a DataFrame with nested structures.
    """
    fields = flatten_schema(df.schema)
    exprs = []
    for field_name, data_type, field_type in fields:
        if field_type == 'primitive':
            exprs.append(col(field_name).alias(field_name.replace('.', '_')))
        elif field_type == 'array_struct':
            # Explode arrays of structs
            exprs.append(explode_outer(col(field_name)).alias(field_name.replace('.', '_')))
        # Additional cases can be added for MapType, etc.
    return df.select(*exprs)

def generate_hash_column(df, exclude_cols):
    """
    Generates a hash column for the DataFrame based on all columns except those in exclude_cols.
    """
    cols_to_hash = [col(c) for c in df.columns if c not in exclude_cols]
    return df.withColumn('hash_value', sha2(to_json(struct(*cols_to_hash)), 256))

def reconcile_dataframes(source_df: DataFrame, target_df: DataFrame, primary_keys: list) -> DataFrame:
    """
    Reconcile source and target DataFrames by comparing all columns.
    """
    # Flatten the DataFrames
    source_flat = flatten_dataframe(source_df)
    target_flat = flatten_dataframe(target_df)

    # Generate hash columns to compare entire rows
    source_flat = generate_hash_column(source_flat, exclude_cols=primary_keys)
    target_flat = generate_hash_column(target_flat, exclude_cols=primary_keys)

    # Join on primary keys
    joined_df = source_flat.alias('src').join(
        target_flat.alias('tgt'),
        on=[source_flat[c] == target_flat[c] for c in primary_keys],
        how='full_outer'
    )

    # Compare hash values to find differences
    diff_df = joined_df.filter(
        (col('src.hash_value') != col('tgt.hash_value')) |
        col('src.hash_value').isNull() |
        col('tgt.hash_value').isNull()
    )

    # Generate differences for each column
    diff_columns = []
    for col_name in set(source_flat.columns).union(set(target_flat.columns)):
        if col_name not in primary_keys + ['hash_value']:
            src_col = col(f'src.{col_name}')
            tgt_col = col(f'tgt.{col_name}')
            diff_col_name = f"{col_name}_diff"
            diff_columns.append(
                when(
                    ~src_col.eqNullSafe(tgt_col),
                    struct(
                        src_col.alias('source'),
                        tgt_col.alias('target')
                    )
                ).alias(diff_col_name)
            )

    # Select primary keys and difference columns
    result_df = diff_df.select(
        *[coalesce(col(f'src.{pk}'), col(f'tgt.{pk}')).alias(pk) for pk in primary_keys],
        *diff_columns
    )

    return result_df

def main():
    """
    Main function to execute the flexible reconciliation process.
    """
    # Initialize Spark session
    spark = initialize_spark()

    # Create sample DataFrames with complex nested structures
    source_df, target_df = create_sample_dataframes(spark)

    # Define primary keys
    primary_keys = ['id']

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
