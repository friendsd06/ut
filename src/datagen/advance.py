# Import necessary libraries
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, min, max, rand, round, lit, expr, array, to_timestamp, from_unixtime
from pyspark.sql.types import *
import sys

# Initialize Spark session
spark = SparkSession.builder \
    .appName("GenericDataGen") \
    .getOrCreate()

# Replace this with your actual input data path
# For demonstration, we'll create a sample input dataframe with various data types
data = [
    (1, 'A', 'X', 'Red', 'Circle', 10, 1.5, '2021-01-01 10:00:00'),
    (2, 'B', 'Y', 'Blue', 'Square', 20, 2.5, '2021-02-01 11:00:00'),
    (3, 'C', 'Z', 'Green', 'Triangle', 30, 3.5, '2021-03-01 12:00:00'),
    (4, 'A', 'X', 'Red', 'Circle', 40, 4.5, '2021-04-01 13:00:00'),
    (5, 'B', 'Y', 'Blue', 'Square', 50, 5.5, '2021-05-01 14:00:00')
]
columns = ['id', 'col1', 'col2', 'col3', 'col4', 'col5', 'col6', 'col7']
input_data = spark.createDataFrame(data, columns)

# Cast 'col7' to TimestampType
input_data = input_data.withColumn('col7', to_timestamp(col('col7')))

# Print input data
print("Input data:")
input_data.show(truncate=False)

# Get the schema from the input data
input_schema = input_data.schema

# Create base DataFrame with desired number of rows
num_rows = 20
# Use range starting from 1 to match 'id' starting from 1
base_df = spark.range(1, num_rows + 1).withColumnRenamed("id", "idx")

# Initialize synthetic_df with base_df
synthetic_df = base_df

# Identify columns by data type
string_columns = [field.name for field in input_schema.fields
                  if isinstance(field.dataType, StringType) and field.name != 'id']
numeric_columns = [field.name for field in input_schema.fields
                   if isinstance(field.dataType, (IntegerType, LongType, DoubleType, FloatType))
                   and field.name != 'id']
timestamp_columns = [field.name for field in input_schema.fields
                     if isinstance(field.dataType, TimestampType) and field.name != 'id']

# Generate string columns without UDFs
for col_name in string_columns:
    # Get unique values for the column
    unique_values = input_data.select(col_name).distinct()
    values_list = [row[col_name] for row in unique_values.collect()]
    values_list.sort()
    num_values = len(values_list)
    # Create array expression with the unique values
    array_expr = array([lit(v) for v in values_list])
    # Map 'idx' to values in the array using modulus operation
    synthetic_df = synthetic_df.withColumn(
        col_name,
        expr(f"element_at({array_expr}, ((idx - 1) % {num_values}) + 1)")
    )

# Generate numeric columns
for col_name in numeric_columns:
    data_type = [field.dataType for field in input_schema.fields if field.name == col_name][0]
    # Get min and max values
    min_value = input_data.agg(min(col(col_name))).collect()[0][0]
    max_value = input_data.agg(max(col(col_name))).collect()[0][0]

    if isinstance(data_type, (IntegerType, LongType)):
        # For integer types, generate random integers within the range
        synthetic_df = synthetic_df.withColumn(
            col_name,
            (rand() * (max_value - min_value + 1) + min_value).cast('integer')
        )
    else:
        # For floating-point types, generate random floats within the range
        synthetic_df = synthetic_df.withColumn(
            col_name,
            round(rand() * (max_value - min_value) + min_value, 2)
        )

# Generate timestamp columns
for col_name in timestamp_columns:
    # Get min and max timestamps in epoch seconds
    min_timestamp = input_data.agg(min(col(col_name))).collect()[0][0].timestamp()
    max_timestamp = input_data.agg(max(col(col_name))).collect()[0][0].timestamp()

    # Generate random timestamps within the range
    synthetic_df = synthetic_df.withColumn(
        col_name,
        expr(f"""
            to_timestamp(
                from_unixtime(
                    cast(rand() * ({max_timestamp} - {min_timestamp}) + {min_timestamp} as bigint)
                )
            )
        """)
    )

# Rename 'idx' back to 'id' to match original schema
synthetic_df = synthetic_df.withColumnRenamed("idx", "id")

# Reorder columns to match the original schema
synthetic_df = synthetic_df.select([field.name for field in input_schema.fields])

# Show synthetic data
print("\nSynthetic data:")
synthetic_df.show(truncate=False)

# Stop the Spark session
spark.stop()
