from pyspark.sql import SparkSession
from dbldatagen import DataGenerator, fakergen
from pyspark.sql.functions import col, expr, broadcast
import time

spark = SparkSession.builder.appName("SkewedBroadcastJoin").getOrCreate()

spark.conf.set("spark.executor.memory", "2g")
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "100m")  # Set a high threshold to force broadcast

# Generate a large left dataset with skewed join key
left_gen = (DataGenerator(spark, name="left_data", rowcount=10_000_000, partitions=50)
            .withColumn("id", expr("uuid()"))
            .withColumn("join_key", expr("""
        case
            when rand() < 0.1 then 'SKEWED_KEY'
            else concat('KEY_', cast(rand() * 1000000 as int))
        end
    """))
            .withColumn("value", expr("rand() * 1000"))
            )

# Generate a large right dataset
right_gen = (DataGenerator(spark, name="right_data", rowcount=5_000_000, partitions=25)
             .withColumn("id", expr("uuid()"))
             .withColumn("join_key", expr("concat('KEY_', cast(rand() * 1000000 as int))"))
             .withColumn("info", fakergen("text", "max_nb_chars=200"))
             )

left_df = left_gen.build()
right_df = right_gen.build()

# Perform a broadcast join
start_time = time.time()
result_df = left_df.join(broadcast(right_df), "join_key")
result_count = result_df.count()
end_time = time.time()

print(f"Broadcast join completed. Result count: {result_count}")
print(f"Execution time: {end_time - start_time:.2f} seconds")
result_df.groupBy("join_key").count().orderBy(col("count").desc()).show(10)