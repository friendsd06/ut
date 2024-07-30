from pyspark.sql import SparkSession
from dbldatagen import DataGenerator, fakergen
from pyspark.sql.functions import col, expr, row_number, sum, rank, dense_rank
from pyspark.sql.window import Window
import time

spark = SparkSession.builder.appName("WindowFunctionMemoryPressure").getOrCreate()

# Set low memory to increase pressure
spark.conf.set("spark.executor.memory", "2g")

# Generate a large dataset
data_gen = (DataGenerator(spark, name="large_dataset", rowcount=20_000_000, partitions=50)
            .withColumn("id", expr("uuid()"))
            .withColumn("user_id", expr("concat('USER_', cast(rand() * 1000000 as int))"))
            .withColumn("event_date", expr("date_sub(current_date(), int(rand() * 365))"))
            .withColumn("value", expr("rand() * 1000"))
            )

df = data_gen.build()

# Define a wide window
window_spec = Window.partitionBy("user_id").orderBy("event_date").rowsBetween(Window.unboundedPreceding, Window.currentRow)

# Apply multiple window functions
start_time = time.time()
result_df = df.withColumn("row_number", row_number().over(window_spec)) \
    .withColumn("running_sum", sum("value").over(window_spec)) \
    .withColumn("rank", rank().over(window_spec)) \
    .withColumn("dense_rank", dense_rank().over(window_spec))

result_count = result_df.count()
end_time = time.time()

print(f"Window functions completed. Result count: {result_count}")
print(f"Execution time: {end_time - start_time:.2f} seconds")
result_df.show(10)