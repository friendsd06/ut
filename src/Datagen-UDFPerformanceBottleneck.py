from pyspark.sql import SparkSession
from dbldatagen import DataGenerator, fakergen
from pyspark.sql.functions import col, expr, udf
from pyspark.sql.types import StringType
import time

spark = SparkSession.builder.appName("UDFPerformanceBottleneck").getOrCreate()

# Generate a large dataset
data_gen = (DataGenerator(spark, name="large_dataset", rowcount=10_000_000, partitions=50)
            .withColumn("id", expr("uuid()"))
            .withColumn("text", fakergen("text", "max_nb_chars=100"))
            )

df = data_gen.build()

# Define a computationally expensive UDF
@udf(StringType())
def expensive_udf(text):
    import hashlib
    result = text
    for _ in range(100):  # Simulate complex processing
        result = hashlib.sha256(result.encode()).hexdigest()
    return result

# Apply the UDF
start_time = time.time()
result_df = df.withColumn("processed_text", expensive_udf(col("text")))
result_count = result_df.count()
end_time = time.time()

print(f"UDF processing completed. Result count: {result_count}")
print(f"Execution time: {end_time - start_time:.2f} seconds")
result_df.show(10)