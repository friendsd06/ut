from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, FloatType

# Initialize Spark session
spark = SparkSession.builder \
    .appName("Create DataFrames") \
    .getOrCreate()

# Define the schema
schema = StructType([
    StructField("ID", IntegerType(), True),
    StructField("Name", StringType(), True),
    StructField("Age", IntegerType(), True),
    StructField("Salary", FloatType(), True),
    StructField("Department", StringType(), True)
])

# Create sample data for DataFrame 1
data1 = [
    (1, "John Doe", 28, 45000.0, "Engineering"),
    (2, "Jane Smith", 34, 55000.0, "Marketing"),
    (3, "Sam Brown", 25, 42000.0, "Sales"),
    (4, "Lucy Gray", 29, 48000.0, "HR"),
    (5, "Mark Black", 31, 53000.0, "Finance")
]

# Create sample data for DataFrame 2
data2 = [
    (6, "Alice Blue", 27, 46000.0, "Engineering"),
    (7, "Bob White", 32, 62000.0, "Marketing"),
    (8, "Charlie Green", 26, 43000.0, "Sales"),
    (9, "Daisy Yellow", 30, 49000.0, "HR"),
    (10, "Eve Purple", 33, 54000.0, "Finance")
]

# Create the DataFrames
df1 = spark.createDataFrame(data1, schema)
df2 = spark.createDataFrame(data2, schema)

# Show the DataFrames
df1.show()
df2.show()


# Source Data
source_data = [
    (1, "John Doe", 28, 45000.0, "Engineering"),
    (2, "Jane Smith", 34, 55000.0, "Marketing"),
    (3, "Sam Brown", 25, 42000.0, "Sales")
]

# Target Data (different Salary)
target_data = [
    (1, "John Doe", 28, 47000.0, "Engineering"),
    (2, "Jane Smith", 34, 56000.0, "Marketing"),
    (3, "Sam Brown", 25, 43000.0, "Sales")
]



---------------

# Source Data
source_data = [
    (4, "Lucy Gray", 29, 48000.0, "HR"),
    (5, "Mark Black", 31, 53000.0, "Finance"),
    (6, "Alice Blue", 27, 46000.0, "Engineering")
]

# Target Data (missing one row)
target_data = [
    (4, "Lucy Gray", 29, 48000.0, "HR"),
    (6, "Alice Blue", 27, 46000.0, "Engineering")
]
