from pyspark.sql import SparkSession
from dbldatagen import DataGenerator, fakergen
from pyspark.sql.functions import col, expr, countDistinct
import time

spark = SparkSession.builder.appName("LargeDistinctCount").getOrCreate()

spark.conf.set("spark.executor.memory", "2g")

# Generate a large dataset with a high cardinality column
data_gen = (DataGenerator(spark, name="high_cardinality_data", rowcount=100_000_000, partitions=100)
            .withColumn("id", expr("uuid()"))
            .withColumn("high_cardinality_col", expr("concat('VALUE_', cast(rand() * 10000000 as int))"))
            .withColumn("group", expr("cast(rand() * 100 as int)"))
            )

df = data_gen.build()

# Perform a distinct count operation
start_time = time.time()
result_df = df.groupBy("group").agg(countDistinct("high_cardinality_col").alias("distinct_count"))
result_df.cache()
result_count = result_df.count()
end_time = time.time()

print(f"Distinct count operation completed. Result count: {result_count}")
print(f"Execution time: {end_time - start_time:.2f} seconds")
result_df.orderBy(col("distinct_count").desc()).show(10)