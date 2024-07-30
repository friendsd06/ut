from pyspark.sql import SparkSession
from dbldatagen import DataGenerator, fakergen
from pyspark.sql.functions import col, expr, udf
from pyspark.sql.types import ArrayType, DoubleType
import time

spark = SparkSession.builder.appName("IterativeAlgorithmAccumulator").getOrCreate()

spark.conf.set("spark.executor.memory", "2g")

# Generate a large dataset
data_gen = (DataGenerator(spark, name="iterative_data", rowcount=10_000_000, partitions=100)
            .withColumn("id", expr("uuid()"))
            .withColumn("value", expr("rand() * 100"))
            )

df = data_gen.build()

# Create an accumulator to store intermediate results
result_accum = spark.sparkContext.accumulator([], ArrayType(DoubleType()))

# Define a UDF for the iterative algorithm
@udf(DoubleType())
def iterative_algorithm(value):
    result = value
    for _ in range(100):  # Perform 100 iterations
        result = (result * 1.01) % 100  # Some arbitrary computation
    result_accum.add([result])
    return result

# Apply the iterative algorithm
start_time = time.time()
result_df = df.withColumn("result", iterative_algorithm(col("value")))
result_df.cache()
result_count = result_df.count()
end_time = time.time()

print(f"Iterative algorithm completed. Result count: {result_count}")
print(f"Execution time: {end_time - start_time:.2f} seconds")
result_df.show(10)

print(f"Accumulator size: {len(result_accum.value)}")