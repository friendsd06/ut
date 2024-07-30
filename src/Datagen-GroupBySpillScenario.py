from pyspark.sql import SparkSession
from dbldatagen import DataGenerator, fakergen
from pyspark.sql.functions import col, expr, count, sum, avg, max, min
import time

# Initialize Spark session
spark = SparkSession.builder.appName("GroupBySpillScenario").getOrCreate()

# Set configurations to increase the likelihood of spills
spark.conf.set("spark.sql.shuffle.partitions", "200")
spark.conf.set("spark.executor.memory", "2g")  # Intentionally set low to induce spills
spark.conf.set("spark.memory.fraction", "0.6")
spark.conf.set("spark.memory.storageFraction", "0.5")
spark.conf.set("spark.sql.shuffle.spill.numElementsForceSpillThreshold", "10000")

# Generate a large dataset with a high cardinality column
data_gen = (DataGenerator(spark, name="high_cardinality_dataset", rowcount=20_000_000, partitions=50)
            .withColumn("id", expr("uuid()"))
            .withColumn("high_cardinality_key", expr("concat('KEY_', cast(rand() * 1000000 as int))"))  # Up to 1 million unique keys
            .withColumn("category", expr("array('A', 'B', 'C', 'D', 'E')[int(rand() * 5)]"))
            .withColumn("value1", expr("rand() * 1000"))
            .withColumn("value2", expr("rand() * 100"))
            .withColumn("timestamp", expr("date_sub(current_timestamp(), int(rand() * 365))"))
            )

# Build the dataset
df = data_gen.build()

print("Dataset generated. Sample data:")
df.show(10, truncate=False)

print("\nDataset statistics:")
df.describe().show()

print("\nUnique count of high_cardinality_key:")
unique_keys = df.select("high_cardinality_key").distinct().count()
print(f"Number of unique keys: {unique_keys}")

# Perform a groupBy operation that's likely to cause spills
print("\nPerforming groupBy operation...")
start_time = time.time()

result_df = df.groupBy("high_cardinality_key", "category").agg(
    count("*").alias("count"),
    sum("value1").alias("sum_value1"),
    avg("value2").alias("avg_value2"),
    max("timestamp").alias("max_timestamp"),
    min("timestamp").alias("min_timestamp")
)

# Force the action to execute
result_count = result_df.count()
end_time = time.time()

print(f"\nGroupBy operation completed. Result count: {result_count}")
print(f"Execution time: {end_time - start_time:.2f} seconds")

print("\nSample of grouped data:")
result_df.orderBy(col("count").desc()).show(20, truncate=False)

# Analyze the distribution of group sizes
print("\nAnalyzing group size distribution:")
group_size_distribution = result_df.groupBy(
    expr("case when count < 10 then 'Very Small (< 10)' "
         "when count < 100 then 'Small (10-99)' "
         "when count < 1000 then 'Medium (100-999)' "
         "when count < 10000 then 'Large (1000-9999)' "
         "else 'Very Large (10000+)' end").alias("group_size_category")
).agg(
    count("*").alias("number_of_groups"),
    sum("count").alias("total_records")
)

group_size_distribution.orderBy("group_size_category").show(truncate=False)

print("\nNote: To confirm spills, check the Spark UI for 'spill' metrics in the shuffle read/write sections of the job stages.")

# Clean up
spark.catalog.clearCache()