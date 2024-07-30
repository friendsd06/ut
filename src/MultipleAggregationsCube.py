from pyspark.sql import SparkSession
from dbldatagen import DataGenerator, fakergen
from pyspark.sql.functions import col, expr, sum, avg, count, cube
import time

spark = SparkSession.builder.appName("MultipleAggregationsCube").getOrCreate()

spark.conf.set("spark.executor.memory", "2g")

# Generate a large dataset with multiple dimensions
data_gen = (DataGenerator(spark, name="multidim_data", rowcount=50_000_000, partitions=100)
            .withColumn("id", expr("uuid()"))
            .withColumn("dim1", expr("array('A', 'B', 'C', 'D', 'E')[int(rand() * 5)]"))
            .withColumn("dim2", expr("array('X', 'Y', 'Z')[int(rand() * 3)]"))
            .withColumn("dim3", expr("cast(rand() * 10 as int)"))
            .withColumn("value1", expr("rand() * 1000"))
            .withColumn("value2", expr("rand() * 100"))
            )

df = data_gen.build()

# Perform multiple aggregations with cube
start_time = time.time()
result_df = df.cube("dim1", "dim2", "dim3").agg(
    sum("value1").alias("sum_value1"),
    avg("value2").alias("avg_value2"),
    count("*").alias("count")
)
result_df.cache()
result_count = result_df.count()
end_time = time.time()

print(f"Cube aggregation completed. Result count: {result_count}")
print(f"Execution time: {end_time - start_time:.2f} seconds")
result_df.orderBy(col("count").desc()).show(10)