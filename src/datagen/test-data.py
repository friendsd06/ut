# Import necessary libraries
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, min, max, rand, round, lit, array, element_at
from pyspark.sql.types import *
import random

# Initialize Spark session
spark = SparkSession.builder \
    .appName("DataGenWithColumn") \
    .getOrCreate()

# For demonstration purposes, create a sample input dataframe
data = [
    (1, 'A', 'X', 'Red', 'Circle', 10, 1.5),
    (2, 'B', 'Y', 'Blue', 'Square', 20, 2.5),
    (3, 'C', 'Z', 'Green', 'Triangle', 30, 3.5),
    (4, 'A', 'X', 'Red', 'Circle', 40, 4.5),
    (5, 'B', 'Y', 'Blue', 'Square', 50, 5.5)
]
columns = ['id', 'col1', 'col2', 'col3', 'col4', 'col5', 'col6']
input_data = spark.createDataFrame(data, columns)

# Print input data
print("Input data:")
input_data.show()

# Get the schema from the input data
input_schema = input_data.schema

# Create base DataFrame with desired number of rows
num_rows = 20
base_df = spark.range(num_rows)

# Function to generate synthetic data for string columns
def generate_string_column(df, col_name, input_df):
    # Get unique values for the column
    unique_values = input_df.select(col_name).distinct().collect()
    values_list = [row[col_name] for row in unique_values]
    values_list.sort()

    # Convert Python list to Spark array
    values_array = array([lit(x) for x in values_list])

    # Use modulo to cycle through values
    return df.withColumn(
        col_name,
        element_at(values_array, (df.id % len(values_list)) + 1)
    )

# Function to generate synthetic data for numeric columns
def generate_numeric_column(df, col_name, input_df, data_type):
    # Get min and max values
    min_value = input_df.agg(min(col(col_name))).collect()[0][0]
    max_value = input_df.agg(max(col(col_name))).collect()[0][0]

    if isinstance(data_type, (IntegerType, LongType)):
        # For integer types
        return df.withColumn(
            col_name,
            (rand() * (max_value - min_value) + min_value).cast('integer')
        )
    else:
        # For floating-point types
        return df.withColumn(
            col_name,
            round(rand() * (max_value - min_value) + min_value, 2)
        )

# Identify columns by data type
string_columns = [field.name for field in input_schema.fields
                  if isinstance(field.dataType, StringType) and field.name != 'id']
numeric_columns = [field.name for field in input_schema.fields
                   if isinstance(field.dataType, (IntegerType, LongType, DoubleType, FloatType))
                   and field.name != 'id']

# Generate synthetic data
synthetic_df = base_df

# Generate string columns
for col_name in string_columns:
    synthetic_df = generate_string_column(synthetic_df, col_name, input_data)

# Generate numeric columns
for col_name in numeric_columns:
    data_type = [field.dataType for field in input_schema.fields if field.name == col_name][0]
    synthetic_df = generate_numeric_column(synthetic_df, col_name, input_data, data_type)

# Show synthetic data
print("\nSynthetic data:")
synthetic_df.show(truncate=False)

# Stop the Spark session
spark.stop()