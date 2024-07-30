from pyspark.sql import SparkSession
from dbldatagen import DataGenerator, fakergen
from pyspark.sql.functions import col, expr
import time

spark = SparkSession.builder.appName("LargeCartesianProduct").getOrCreate()

spark.conf.set("spark.executor.memory", "2g")

# Generate two moderately sized datasets
left_gen = (DataGenerator(spark, name="left_data", rowcount=100_000, partitions=10)
            .withColumn("id", expr("uuid()"))
            .withColumn("value", expr("rand() * 100"))
            )

right_gen = (DataGenerator(spark, name="right_data", rowcount=100_000, partitions=10)
             .withColumn("id", expr("uuid()"))
             .withColumn("info", fakergen("text", "max_nb_chars=50"))
             )

left_df = left_gen.build()
right_df = right_gen.build()

# Perform a cartesian product
start_time = time.time()
result_df = left_df.crossJoin(right_df)
result_count = result_df.count()
end_time = time.time()

print(f"Cartesian product completed. Result count: {result_count}")
print(f"Execution time: {end_time - start_time:.2f} seconds")
result_df.show(10)