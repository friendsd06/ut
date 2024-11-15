# Import necessary libraries
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, min, max
import dbldatagen as dg
import sys

# Initialize Spark session
spark = SparkSession.builder \
    .appName("DBDataGenExample") \
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

# Identify columns (excluding 'id' if present)
data_columns = [field.name for field in input_schema.fields if field.name != 'id']

# Separate columns by data type
string_columns = [field.name for field in input_schema.fields if str(field.dataType) == 'StringType' and field.name != 'id']
numeric_columns = [field.name for field in input_schema.fields if str(field.dataType) in ['IntegerType', 'LongType', 'DoubleType', 'FloatType'] and field.name != 'id']

# Initialize the DataGenerator with an 'id' column
data_generator = dg.DataGenerator(
    sparkSession=spark,
    name="synthetic_data",
    rows=20,  # Adjust the number of rows as needed
    schema=input_schema
).withIdOutput()

# For string columns, extract unique values and map them deterministically
for col_name in string_columns:
    # Get unique values for the column
    unique_values = input_data.select(col_name).distinct().collect()
    # Convert to a list of values
    values_list = [row[col_name] for row in unique_values]
    # Sort the values to maintain consistent ordering
    values_list.sort()
    # Convert to a comma-separated string for use in expression
    values_list_str = ', '.join([f"'{val}'" for val in values_list])
    # Get the number of unique values
    num_values = len(values_list)
    # Use 'element_at' to map 'id' to values in the array deterministically
    data_generator = data_generator.withColumnSpec(
        col_name,
        expr=f"element_at(array({values_list_str}), ((id - 1) % {num_values}) + 1)"
    )

# For numeric columns, generate values within the min and max range
for col_name in numeric_columns:
    # Get min and max values from the input data
    min_value = input_data.agg(min(col(col_name))).collect()[0][0]
    max_value = input_data.agg(max(col(col_name))).collect()[0][0]
    # Check the data type
    data_type = [field.dataType for field in input_schema.fields if field.name == col_name][0]
    # Configure the data generator for numeric columns
    if str(data_type) in ['IntegerType', 'LongType']:
        data_generator = data_generator.withColumnSpec(
            col_name,
            minValue=min_value,
            maxValue=max_value,
            step=1,  # Increment by 1 for integers
            random=True
        )
    elif str(data_type) in ['DoubleType', 'FloatType']:
        data_generator = data_generator.withColumnSpec(
            col_name,
            minValue=min_value,
            maxValue=max_value,
            random=True
        )

# Generate the synthetic data
synthetic_data = data_generator.build()

# Show synthetic data
print("Synthetic data:")
synthetic_data.show(truncate=False)

# Stop the Spark session
spark.stop()
