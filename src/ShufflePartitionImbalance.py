from pyspark.sql import SparkSession
from dbldatagen import DataGenerator, fakergen
from pyspark.sql.functions import col, expr
import time

spark = SparkSession.builder.appName("ShufflePartitionImbalance").getOrCreate()

# Set a very low number of shuffle partitions
spark.conf.set("spark.sql.shuffle.partitions", "10")

# Generate a large dataset
data_gen = (DataGenerator(spark, name="large_dataset", rowcount=100_000_000, partitions=100)
            .withColumn("id", expr("uuid()"))
            .withColumn("category", expr("array('A', 'B', 'C', 'D', 'E')[int(rand() * 5)]"))
            .withColumn("value", expr("rand() * 1000"))
            )

df = data_gen.build()

# Perform a groupBy operation that will trigger a shuffle
start_time = time.time()
result_df = df.groupBy("category").sum("value")
result_count = result_df.count()
end_time = time.time()

print(f"GroupBy operation completed. Result count: {result_count}")
print(f"Execution time: {end_time - start_time:.2f} seconds")
result_df.show()

print("Note: Check Spark UI for uneven task durations in the shuffle stage.")