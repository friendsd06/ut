from pyspark.sql import SparkSession
from dbldatagen import DataGenerator, fakergen
from pyspark.sql.functions import col, expr
import time

spark = SparkSession.builder.appName("LargeSortManyPartitions").getOrCreate()

spark.conf.set("spark.executor.memory", "2g")
spark.conf.set("spark.sql.shuffle.partitions", "1000")  # Set a high number of shuffle partitions

# Generate a large dataset
data_gen = (DataGenerator(spark, name="large_data", rowcount=100_000_000, partitions=200)
            .withColumn("id", expr("uuid()"))
            .withColumn("value", expr("rand() * 1000000"))
            .withColumn("category", expr("array('A', 'B', 'C', 'D', 'E')[int(rand() * 5)]"))
            )

df = data_gen.build()

# Perform a large sort operation
start_time = time.time()
result_df = df.sort("value", "category")
result_df.cache()
result_count = result_df.count()
end_time = time.time()

print(f"Sort operation completed. Result count: {result_count}")
print(f"Execution time: {end_time - start_time:.2f} seconds")
result_df.show(10)