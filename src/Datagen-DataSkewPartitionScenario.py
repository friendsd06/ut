from pyspark.sql import SparkSession
from dbldatagen import DataGenerator
from pyspark.sql.functions import col, expr, when, count, size, collect_list, avg, length

# Initialize Spark session
spark = SparkSession.builder.appName("DataSkewPartitionScenario").getOrCreate()

# Set a fixed number of partitions to make the skew more apparent
spark.conf.set("spark.sql.shuffle.partitions", "100")

# Generate a large dataset with highly skewed data
data_gen = (DataGenerator(spark, name="skewed_dataset", rows=10_000_000, partitions=100)
            .withColumn("id", "string", expr("uuid()"))
            .withColumn("partition_key", "string", expr("""
        case
            when rand() < 0.001 then '1'  # 0.1% of data
            when rand() < 0.01 then '2'   # ~0.9% of data
            when rand() < 0.1 then '3'    # ~9% of data
            when rand() < 0.3 then '4'    # ~20% of data
            when rand() < 0.6 then '5'    # ~30% of data
            else cast(5 + int(rand() * 95) as string)  # Rest spread across 95 partitions
        end
    """))
            .withColumn("timestamp", "timestamp", expr("date_sub(current_timestamp(), int(rand() * 365))"))
            .withColumn("value", "double", expr("""
        case
            when partition_key = '1' then rand() * 1000000
            when partition_key = '2' then rand() * 100000
            when partition_key = '3' then rand() * 10000
            when partition_key = '4' then rand() * 1000
            when partition_key = '5' then rand() * 100
            else rand() * 10
        end
    """))
            .withColumn("payload", "string", expr("repeat('x', 100 + int(rand() * 900))"))  # Variable-length string to increase row size
            )

# Build the dataset
df = data_gen.build()

print("Dataset generated. Sample data:")
df.show(10, truncate=False)

print("\nPartition key distribution:")
partition_distribution = df.groupBy("partition_key").agg(count("*").alias("count")).orderBy(col("count").desc())
partition_distribution.show(100)

# Repartition the data based on the skewed partition_key
skewed_df = df.repartition(100, "partition_key")

# Analyze partition sizes
def analyze_partitions(df):
    return df.groupBy(spark.sparkContext.partition_id()).agg(
        count("*").alias("row_count"),
        (size(collect_list("id")) * avg(length("payload"))).alias("estimated_size_bytes")
    ).orderBy("partition_id")

partition_analysis = analyze_partitions(skewed_df)
partition_analysis.show(100)

# Simulate processing time differences
def process_partition(df):
    # Simulate more processing time for larger partitions
    return df.withColumn("processed_value",
                         when(col("partition_key").isin("1", "2", "3"), expr("pow(value, 2) * 1000"))
                         .otherwise(col("value")))

print("\nProcessing partitions...")
processed_df = skewed_df.groupBy("partition_key").applyInPandas(process_partition, skewed_df.schema)

# Analyze processing times (this is a simplified simulation)
processing_times = processed_df.groupBy("partition_key").agg(
    count("*").alias("row_count"),
    (count("*") * when(col("partition_key").isin("1", "2", "3"), 1000).otherwise(1)).alias("relative_processing_time")
).orderBy(col("relative_processing_time").desc())

print("\nSimulated processing times per partition:")
processing_times.show(100)

print("Analysis complete.")

# Clean up
spark.catalog.clearCache()