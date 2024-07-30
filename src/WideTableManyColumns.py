from pyspark.sql import SparkSession
from dbldatagen import DataGenerator, fakergen
from pyspark.sql.functions import col, expr
import time

spark = SparkSession.builder.appName("WideTableManyColumns").getOrCreate()

spark.conf.set("spark.executor.memory", "2g")

# Generate a dataset with many columns
num_columns = 1000
data_gen = (DataGenerator(spark, name="wide_data", rowcount=1_000_000, partitions=50)
            .withColumn("id", expr("uuid()"))
            )

for i in range(num_columns):
    data_gen = data_gen.withColumn(f"col_{i}", expr("rand()"))

df = data_gen.build()

# Perform an operation that requires shuffling all columns
start_time = time.time()
result_df = df.select(*[expr(f"sum(col_{i})").alias(f"sum_{i}") for i in range(num_columns)])
result_df.show()
end_time = time.time()

print(f"Wide table operation completed.")
print(f"Execution time: {end_time - start_time:.2f} seconds")