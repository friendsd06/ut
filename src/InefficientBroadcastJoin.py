from pyspark.sql import SparkSession
from dbldatagen import DataGenerator, fakergen
from pyspark.sql.functions import col, expr, broadcast
import time

spark = SparkSession.builder.appName("InefficientBroadcastJoin").getOrCreate()

# Set a high broadcast join threshold
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", str(100 * 1024 * 1024))  # 100 MB

# Generate two datasets
small_gen = (DataGenerator(spark, name="small_dataset", rowcount=1_000_000, partitions=10)
             .withColumn("id", expr("uuid()"))
             .withColumn("join_key", expr("cast(rand() * 1000 as int)"))
             .withColumn("value", expr("rand() * 100"))
             )

large_gen = (DataGenerator(spark, name="large_dataset", rowcount=10_000_000, partitions=50)
             .withColumn("id", expr("uuid()"))
             .withColumn("join_key", expr("cast(rand() * 1000 as int)"))
             .withColumn("data", fakergen("text", "max_nb_chars=1000"))  # Large text field to increase row size
             )

small_df = small_gen.build()
large_df = large_gen.build()

# Force a broadcast join
start_time = time.time()
result_df = large_df.join(broadcast(small_df), "join_key")
result_count = result_df.count()
end_time = time.time()

print(f"Broadcast join completed. Result count: {result_count}")
print(f"Execution time: {end_time - start_time:.2f} seconds")
result_df.show(10)

print("Note: Check Spark UI for large broadcast variable size and potential OOM errors.")