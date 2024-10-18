from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
from pyspark.sql.functions import col, struct

def flatten_and_drop_column(df: DataFrame, column_to_flatten: str) -> DataFrame:
    # Check if the column exists in the DataFrame
    if column_to_flatten not in df.columns:
        raise ValueError(f"Column '{column_to_flatten}' not found in the DataFrame")

    # Get the schema of the struct column
    struct_schema = df.schema[column_to_flatten].dataType

    # Create a list of new column names
    new_columns = [
        col(f"{column_to_flatten}.{field.name}").alias(f"{column_to_flatten}_{field.name}")
        for field in struct_schema.fields
    ]

    # Select all columns except the one to flatten, then add the flattened columns
    flattened_df = df.select(
        *[c for c in df.columns if c != column_to_flatten],
        *new_columns
    )

    return flattened_df

# Test the function
if __name__ == "__main__":
    # Create a SparkSession
    spark = SparkSession.builder.appName("FlattenColumnTest").getOrCreate()

    # Create a sample DataFrame with a nested column
    schema = StructType([
        StructField("id", IntegerType(), False),
        StructField("info", StructType([
            StructField("name", StringType(), True),
            StructField("age", IntegerType(), True)
        ]), True)
    ])

    data = [
        (1, ("Alice", 30)),
        (2, ("Bob", 35)),
        (3, ("Charlie", 25))
    ]

    df = spark.createDataFrame(data, schema)

    print("Original DataFrame:")
    df.show()
    df.printSchema()

    # Use the function to flatten the 'info' column
    flattened_df = flatten_and_drop_column(df, "info")

    print("\nFlattened DataFrame:")
    flattened_df.show()
    flattened_df.printSchema()

    # Test case: Check if the flattened DataFrame has the expected structure
    assert "info" not in flattened_df.columns, "Original 'info' column should be dropped"
    assert "info_name" in flattened_df.columns, "Flattened 'info_name' column should exist"
    assert "info_age" in flattened_df.columns, "Flattened 'info_age' column should exist"

    print("\nAll tests passed successfully!")

    # Stop the SparkSession
    spark.stop()