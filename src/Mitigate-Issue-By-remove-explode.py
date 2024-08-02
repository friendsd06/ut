from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, lit, rand, count, sum

# Initialize Spark Session
spark = SparkSession.builder.appName("ReducedSkewScenario").getOrCreate()

# Set a large number of shuffle partitions to make skew more apparent
spark.conf.set("spark.sql.shuffle.partitions", "200")

# Disable broadcast joins to force shuffle joins
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")

# Function to generate highly skewed data
def generate_skewed_data(num_records, num_keys, skew_percentage):
    skewed_key = 0
    return (spark.range(num_records)
            .withColumn("key",
                        when(rand() < skew_percentage, lit(skewed_key))
                        .otherwise((col("id") % num_keys).cast("int")))
            .withColumn("value", when(col("key") == skewed_key, lit(1000000))
                        .otherwise(rand() * 100))
            .withColumn("nested_data", rand() * 10))  # Single value instead of array

# Generate datasets
num_records_large = 50000000  # 50 million
num_records_small = 1000000   # 1 million
num_keys = 1000
skew_percentage = 0.9  # 90% of data goes to one key

df_large = generate_skewed_data(num_records_large, num_keys, skew_percentage)
df_small = generate_skewed_data(num_records_small, num_keys, skew_percentage)

# Alias the DataFrames to avoid ambiguity
df_large = df_large.alias("large")
df_small = df_small.alias("small")

# Cache the smaller dataset
df_small.cache().count()

# Perform a skewed join operation
result = (df_large.join(df_small, df_large.key == df_small.key)
          .groupBy(df_large.key)
          .agg(count("*").alias("count"),
               sum(df_large.value).alias("sum_value_large"),
               sum(df_small.value).alias("sum_value_small"),
               sum(df_large.nested_data).alias("sum_nested")))

# Force computation
print("Starting computation...")
result.explain(mode="extended")
result_count = result.count()
print(f"Result count: {result_count}")

# Show the distribution of data across keys
print("Data distribution across keys:")
df_large.groupBy("key").count().orderBy(col("count").desc()).show(10)

# Show a sample of results
print("Sample results:")
result.orderBy(col("sum_value_large").desc()).show(10)

# Stop the Spark session
spark.stop()