from pyspark.sql import SparkSession
from dbldatagen import DataGenerator, fakergen
from pyspark.sql.functions import col, expr
import time

spark = SparkSession.builder.appName("MultipleJoinsDataExplosion").getOrCreate()

spark.conf.set("spark.executor.memory", "2g")
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")  # Disable broadcast joins

# Generate multiple datasets
data_gens = []
for i in range(5):
    data_gens.append(
        DataGenerator(spark, name=f"data_{i}", rowcount=1_000_000, partitions=20)
            .withColumn("id", expr("uuid()"))
            .withColumn("join_key", expr("cast(rand() * 1000 as int)"))
            .withColumn(f"value_{i}", expr("rand() * 100"))
    )

dfs = [gen.build() for gen in data_gens]

# Perform multiple joins
start_time = time.time()
result_df = dfs[0]
for i in range(1, len(dfs)):
    result_df = result_df.join(dfs[i], "join_key")

result_count = result_df.count()
end_time = time.time()

print(f"Multiple joins completed. Result count: {result_count}")
print(f"Execution time: {end_time - start_time:.2f} seconds")
result_df.show(10)