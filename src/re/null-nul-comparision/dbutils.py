# Define source and target paths
source_path = "s3a://your-bucket/path/to/data/partition43543/"
target_path = "s3a://your-bucket/path/to/data/partition45654/"

# Use dbutils.fs.cp to copy files from source to target
dbutils.fs.cp(source_path, target_path, recurse=True)


def clean_redundant_nulls(df, columns):
    for column in columns:
        # Replace multiple ", , " with a single empty string
        df = df.withColumn(column, regexp_replace(col(column), r"(, )+", ""))
        # Remove leading or trailing commas
        df = df.withColumn(column, regexp_replace(col(column), r"^,|,$", ""))

    return df


def clean_redundant_nulls(df, columns):
    for column in columns:
        # Replace multiple "null, null" with a single "null"
        df = df.withColumn(column, regexp_replace(col(column), r"(null,\s*)+null", "null"))

        # Alternatively, replace with an empty string
        # df = df.withColumn(column, regexp_replace(col(column), r"(null,\s*)+null", ""))

    return df


from pyspark.sql.functions import col, concat, coalesce, lit, when, regexp_replace, concat_ws
from pyspark.sql import SparkSession
from pyspark.sql.types import StringType
from functools import reduce

# Initialize SparkSession
spark = SparkSession.builder \
    .appName("Reconciliation with Null and 'null' String Check") \
    .getOrCreate()

# Function to identify scalar and struct columns
def identify_column_types(df):
    scalar_cols = [f.name for f in df.schema.fields if str(f.dataType) not in ["ArrayType", "StructType"]]
    struct_cols = [f.name for f in df.schema.fields if "StructType" in str(f.dataType)]
    return scalar_cols, struct_cols

# Function to add prefix to columns
def add_prefix(df, prefix):
    return df.select([col(c).alias(f"{prefix}_{c}") for c in df.columns])

# Function to replace "null" strings with real nulls in all columns
def replace_null_strings(df, columns):
    for column in columns:
        df = df.withColumn(column, when(col(column) == "null", None).otherwise(col(column)))
    return df

# Function to compare struct columns
def compare_structs(joined_df, struct_cols):
    struct_comparisons = []

    for struct_col in struct_cols:
        sub_fields = joined_df.select(f"source_{struct_col}.*").columns
        sub_field_comparisons = []

        for sub_field in sub_fields:
            source_sub_col = col(f"source_{struct_col}.{sub_field}")
            target_sub_col = col(f"target_{struct_col}.{sub_field}")

            # Define the difference expression
            diff_expression = when(
                ~source_sub_col.eqNullSafe(target_sub_col),
                concat(
                    lit(f"{struct_col}.{sub_field}: Source = "),
                    coalesce(when(source_sub_col == "null", None).otherwise(source_sub_col.cast("string")), lit("")),
                    lit(", Target = "),
                    coalesce(when(target_sub_col == "null", None).otherwise(target_sub_col.cast("string")), lit(""))
                )
            ).otherwise("")

            sub_field_comparisons.append(diff_expression)

        # Concatenate all sub-field differences for the struct column
        struct_diff_col = concat_ws(
            ".", *[col for col in sub_field_comparisons if col != ""]
        ).alias(f"{struct_col}_diff")

        struct_comparisons.append(struct_diff_col)

    return struct_comparisons

# Function to compare scalar columns
def compare_scalars(joined_df, scalar_cols):
    scalar_comparisons = []

    for scalar_col in scalar_cols:
        source_col = col(f"source_{scalar_col}")
        target_col = col(f"target_{scalar_col}")

        diff_col = when(
            ~source_col.eqNullSafe(target_col),
            concat(
                lit(f"{scalar_col}: Source = "),
                coalesce(when(source_col == "null", None).otherwise(source_col.cast("string")), lit("")),
                lit(", Target = "),
                coalesce(when(target_col == "null", None).otherwise(target_col.cast("string")), lit(""))
            )
        ).otherwise("").alias(f"{scalar_col}_diff")

        scalar_comparisons.append(diff_col)

    return scalar_comparisons

# Function to perform reconciliation
def reconcile_main_entity(source_df, target_df, primary_keys):
    # Get the list of scalar and struct columns
    scalar_cols, struct_cols = identify_column_types(source_df)

    # Add prefix to the source and target columns
    source_df_prefixed = add_prefix(source_df, "source")
    target_df_prefixed = add_prefix(target_df, "target")

    # Join on primary keys
    join_conditions = [col(f"source_{pk}").eqNullSafe(col(f"target_{pk}")) for pk in primary_keys]
    joined_df = source_df_prefixed.join(target_df_prefixed, on=join_conditions, how="full_outer")

    # Compare scalar and struct columns
    scalar_comparisons = compare_scalars(joined_df, scalar_cols)
    struct_comparisons = compare_structs(joined_df, struct_cols)

    # Create primary key columns
    primary_key_cols = [coalesce(col(f"source_{pk}"), col(f"target_{pk}")).alias(pk) for pk in primary_keys]

    # Select all columns for final DataFrame
    final_df = joined_df.select(*primary_key_cols, *scalar_comparisons, *struct_comparisons)

    # Replace "null" strings with real nulls in comparison columns
    comparison_columns = [c for c in final_df.columns if "_diff" in c]
    final_df = replace_null_strings(final_df, comparison_columns)

    # Apply filter: keep only rows where at least one comparison column is not null or "null"
    non_null_condition = reduce(lambda x, y: x | (col(y).isNotNull() & (col(y) != "null")), comparison_columns, lit(False))
    final_df = final_df.filter(non_null_condition)

    return final_df

# Example source and target DataFrames (to be replaced with actual data)
data = [
    (1, "A", 100, None),
    (2, "B", 200, {"field1": "X", "field2": "Y"}),
    (3, None, 300, {"field1": "Z", "field2": "W"})
]

schema = "id INT, name STRING, amount INT, details STRUCT<field1: STRING, field2: STRING>"
source_df = spark.createDataFrame(data, schema)

target_data = [
    (1, "A", 105, None),
    (2, "C", 200, {"field1": "X", "field2": "Y"}),
    (3, "null", 300, {"field1": "Z", "field2": None})
]

target_df = spark.createDataFrame(target_data, schema)

# Reconciliation process
primary_keys = ["id"]
final_df = reconcile_main_entity(source_df, target_df, primary_keys)

# Show the final result
final_df.show(truncate=False)

