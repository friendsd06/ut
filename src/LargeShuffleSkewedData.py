from pyspark.sql import SparkSession
from dbldatagen import DataGenerator, fakergen
from pyspark.sql.functions import col, expr, count
import time

spark = SparkSession.builder.appName("LargeShuffleSkewedData").getOrCreate()

# Set a low memory threshold to increase likelihood of spills
spark.conf.set("spark.executor.memory", "2g")
spark.conf.set("spark.memory.fraction", "0.6")
spark.conf.set("spark.memory.storageFraction", "0.5")

# Generate a large dataset with skewed keys
data_gen = (DataGenerator(spark, name="skewed_data", rowcount=100_000_000, partitions=100)
            .withColumn("id", expr("uuid()"))
            .withColumn("key", expr("""
        case
            when rand() < 0.1 then 'HOT_KEY'
            else concat('KEY_', cast(rand() * 1000000 as int))
        end
    """))
            .withColumn("value", expr("rand() * 1000"))
            )

df = data_gen.build()

# Perform a groupBy operation that will trigger a large shuffle
start_time = time.time()
result_df = df.groupBy("key").agg(count("*").alias("count"), expr("sum(value)").alias("sum_value"))
result_count = result_df.count()
end_time = time.time()

print(f"GroupBy operation completed. Result count: {result_count}")
print(f"Execution time: {end_time - start_time:.2f} seconds")
result_df.orderBy(col("count").desc()).show(10)

print("Note: Check Spark UI for spill metrics in the shuffle read/write sections.")