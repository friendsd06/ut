from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# Initialize SparkSession
spark = SparkSession.builder \
    .appName("Read CSV with Custom Schema") \
    .getOrCreate()

# Define the schema
schema = StructType([
    StructField("Name", StringType(), True),
    StructField("Age", IntegerType(), True),
    StructField("City", StringType(), True)
])

# Read the CSV file with the defined schema
df = spark.read.csv("path/to/your/csvfile.csv", header=True, schema=schema)

# Show the DataFrame
df.show()
