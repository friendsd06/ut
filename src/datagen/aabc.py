# Import necessary libraries
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType
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

# Initialize the DataGenerator with the input schema
data_generator = dg.DataGenerator(sparkSession=spark, name="synthetic_data", rows=10000, schema=input_schema)

# Customize data generation for specific columns (optional)
data_generator = data_generator \
    .withColumnSpec("id", minValue=1, maxValue=1000000, step=1) \
    .withColumnSpec("age", minValue=18, maxValue=65, random=True) \
    .withColumnSpec("salary", minValue=30000, maxValue=150000, random=True) \
    .withColumnSpec("join_date", begin="2015-01-01", end="2021-12-31", interval="1 day", random=True) \
    .withColumnSpec("name", prefix="Name_", random=True)

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
