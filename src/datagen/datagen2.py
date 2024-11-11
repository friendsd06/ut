# Import necessary libraries
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count
import dbldatagen as dg
import sys

# Initialize Spark session
spark = SparkSession.builder \
    .appName("DBDataGenExample") \
    .getOrCreate()

# Path to your input data file (CSV format)
input_file_path = "/path/to/your/data/input_data.csv"

# Read data from the file
try:
    input_data = spark.read.format("csv") \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .load(input_file_path)
    print("Input data schema:")
    input_data.printSchema()
except Exception as e:
    print(f"Error reading input data: {e}")
    spark.stop()
    sys.exit(1)

# Get the schema from the input data
input_schema = input_data.schema

# Identify string columns
string_columns = [field.name for field in input_schema.fields if str(field.dataType) == 'StringType']

# Initialize the DataGenerator with the input schema
data_generator = dg.DataGenerator(sparkSession=spark, name="synthetic_data", rows=10000, schema=input_schema)

# For string columns, extract actual values and use them in data generation
for col_name in string_columns:
    # Collect unique values and their frequencies
    value_counts = input_data.groupBy(col_name).count()
    total_count = input_data.count()

    # Collect values and their probabilities
    value_prob_df = value_counts.withColumn('probability', col('count') / total_count)
    value_prob_list = value_prob_df.select(col_name, 'probability').collect()

    # Prepare lists of values and weights
    values_list = []
    weights_list = []
    for row in value_prob_list:
        value = row[col_name]
        probability = row['probability']
        if value is not None:
            values_list.append(value)
            weights_list.append(probability)

    # Configure data generator to use these values and weights
    if values_list:
        data_generator = data_generator.withColumnSpec(col_name, values=values_list, weights=weights_list, random=True)
    else:
        # If no values are available, handle accordingly
        data_generator = data_generator.withColumnSpec(col_name, prefix="Value_", random=True)

# Customize data generation for non-string columns (e.g., integers, dates)
# Example for integer and date columns
for field in input_schema.fields:
    col_name = field.name
    data_type = str(field.dataType)
    if col_name not in string_columns:
        if data_type == 'IntegerType' or data_type == 'LongType':
            data_generator = data_generator.withColumnSpec(col_name, minValue=1, maxValue=1000, random=True)
        elif data_type == 'DoubleType' or data_type == 'FloatType':
            data_generator = data_generator.withColumnSpec(col_name, minValue=0.0, maxValue=100.0, random=True)
        elif data_type == 'DateType' or data_type == 'TimestampType':
            data_generator = data_generator.withColumnSpec(col_name, begin="2015-01-01", end="2021-12-31", random=True)
        # Add other data types as needed

# Generate the data
try:
    generated_data = data_generator.build()
    print("Generated data sample:")
    generated_data.show(5)
except Exception as e:
    print(f"Error generating data: {e}")
    spark.stop()
    sys.exit(1)

# Write the generated data to a CSV file
output_file_path = "/path/to/your/data/generated_data.csv"
try:
    generated_data.write.format("csv") \
        .option("header", "true") \
        .mode("overwrite") \
        .save(output_file_path)
    print(f"Generated data saved to: {output_file_path}")
except Exception as e:
    print(f"Error writing generated data to file: {e}")
    spark.stop()
    sys.exit(1)

# Stop the Spark session
spark.stop()
