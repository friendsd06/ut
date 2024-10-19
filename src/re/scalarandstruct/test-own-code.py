from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# Initialize Spark session
spark = SparkSession.builder.master("local").appName("DataFrame Comparison").getOrCreate()

# Create sample data for source DataFrame
source_data = [
    (1, "A", 10, {"sub1": "S1", "sub2": "S2"}),
    (2, "B", 20, {"sub1": "S3", "sub2": "S4"}),
    (3, "C", 30, {"sub1": "S5", "sub2": "S6"}),
    (4, "D", 40, {"sub1": "S7", "sub2": "S8"})
]

# Create sample data for target DataFrame
target_data = [
    (1, "A", 15, {"sub1": "S1", "sub2": "S2_modified"}),  # Different scalar and struct field
    (2, "B_modified", 20, {"sub1": "S3_modified", "sub2": "S4"}),  # Different scalar and struct field
    (3, "C", 35, {"sub1": "S5", "sub2": "S6"}),  # Different scalar field
    (5, "E", 50, {"sub1": "S9", "sub2": "S10"})  # New record in target
]

# Define schema with nested struct for both DataFrames
schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("value", IntegerType(), True),
    StructField("details", StructType([
        StructField("sub1", StringType(), True),
        StructField("sub2", StringType(), True)
    ]), True)
])

# Create source and target DataFrames
source_df = spark.createDataFrame(source_data, schema=schema)
target_df = spark.createDataFrame(target_data, schema=schema)

# Define primary keys for comparison
primary_keys = ['id']

# Function to identify struct and scalar columns dynamically
def identify_column_types(df):
    struct_cols = [field.name for field in df.schema.fields if isinstance(field.dataType, StructType)]
    scalar_cols = [field.name for field in df.schema.fields if not isinstance(field.dataType, StructType)]
    return scalar_cols, struct_cols

# Function to add prefixes for source and target columns
def add_prefix(df, prefix):
    return df.select([F.col(col).alias(f"{prefix}{col}") for col in df.columns])

# Function to compare scalar columns
def compare_scalars(joined_df, scalar_cols):
    return [
        F.when(
            F.coalesce(F.col(f"source_{col}"), F.lit("null")) != F.coalesce(F.col(f"target_{col}"), F.lit("null")),
            F.concat(F.lit(f"{col}: Source = "), F.coalesce(F.col(f"source_{col}"), F.lit("null")),
                     F.lit(", Target = "), F.coalesce(F.col(f"target_{col}"), F.lit("null")))
        ).alias(f"{col}_diff")
        for col in scalar_cols
    ]

# Function to compare struct columns
def compare_structs(joined_df, struct_cols):
    struct_comparisons = []
    for struct_col in struct_cols:
        sub_fields = joined_df.select(f"source_{struct_col}.*").columns
        sub_field_comparisons = [
            F.when(
                F.coalesce(F.col(f"source_{struct_col}.{sub_field}"), F.lit("null")) !=
                F.coalesce(F.col(f"target_{struct_col}.{sub_field}"), F.lit("null")),
                F.concat(
                    F.lit(f"{struct_col}.{sub_field}: Source = "),
                    F.coalesce(F.col(f"source_{struct_col}.{sub_field}"), F.lit("null")),
                    F.lit(", Target = "),
                    F.coalesce(F.col(f"target_{struct_col}.{sub_field}"), F.lit("null"))
                )
            ).otherwise("")
            for sub_field in sub_fields
        ]
        struct_comparisons.append(
            F.concat_ws(", ", *sub_field_comparisons).alias(f"{struct_col}_diff")
        )
    return struct_comparisons

# Main function to perform dataframe comparison
def compare_dataframes(source_df, target_df, primary_keys):
    # Add prefixes to distinguish source and target columns
    source_df_prefixed = add_prefix(source_df, "source_")
    target_df_prefixed = add_prefix(target_df, "target_")

    # Join dataframes on primary keys with coalescing to handle nulls
    join_conditions = [
        F.coalesce(F.col(f"source_{pk}"), F.lit("null")) == F.coalesce(F.col(f"target_{pk}"), F.lit("null"))
        for pk in primary_keys
    ]
    joined_df = source_df_prefixed.join(target_df_prefixed, on=join_conditions, how="full_outer")

    # Identify scalar and struct columns in source dataframe
    scalar_cols, struct_cols = identify_column_types(source_df)

    # Compare scalar and struct columns
    scalar_comparisons = compare_scalars(joined_df, scalar_cols)
    struct_comparisons = compare_structs(joined_df, struct_cols)

    # Select primary keys and comparison results
    final_df = joined_df.select(*primary_keys, *scalar_comparisons, *struct_comparisons)

    return final_df

# Perform comparison using the defined function
result_df = compare_dataframes(source_df, target_df, primary_keys)

# Display the comparison results
import ace_tools as tools

tools.display_dataframe_to_user(name="DataFrame Comparison Result", dataframe=result_df)
