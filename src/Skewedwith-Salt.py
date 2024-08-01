from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, lit, rand, explode, array, count, sum, concat, hash, abs
import random

# Initialize Spark Session
spark = SparkSession.builder.appName("SaltedSkewScenario").getOrCreate()

# Set a large number of shuffle partitions
spark.conf.set("spark.sql.shuffle.partitions", "200")

# Define the number of salts (adjust based on your cluster size and data distribution)
num_salts = 50

# Function to generate highly skewed data with salting
def generate_skewed_data_with_salt(num_records, num_keys, skew_percentage, df_name):
    skewed_key = 0
    return (spark.range(num_records)
            .withColumn("original_key",
                        when(rand() < skew_percentage, lit(skewed_key))
                        .otherwise((col("id") % num_keys).cast("int")))
            .withColumn("salt", (abs(hash(col("id"))) % num_salts).cast("int"))
            .withColumn("salted_key",
                        when(col("original_key") == skewed_key,
                             concat(col("original_key").cast("string"), lit("_"), col("salt").cast("string")))
                        .otherwise(col("original_key").cast("string")))
            .withColumn("value", when(col("original_key") == skewed_key, lit(1000000))
                        .otherwise(rand() * 100))
            .withColumn("nested_data", array([rand() * 10 for _ in range(10)]))
            .select("salted_key", "original_key", "value", "nested_data")
            .alias(df_name))

# Generate datasets
num_records_large = 50000000  # 50 million
num_records_small = 1000000   # 1 million
num_keys = 1000
skew_percentage = 0.9  # 90% of data goes to one key

df_large = generate_skewed_data_with_salt(num_records_large, num_keys, skew_percentage, "large")
df_small = generate_skewed_data_with_salt(num_records_small, num_keys, skew_percentage, "small")

# Cache the smaller dataset
df_small.cache().count()

# Perform a skewed join operation with salting
result = (df_large.join(df_small, df_large.salted_key == df_small.salted_key)
          .withColumn("exploded_data", explode(df_large.nested_data))
          .groupBy(df_large.original_key)
          .agg(count("*").alias("count"),
               sum(df_large.value).alias("sum_value_large"),
               sum(df_small.value).alias("sum_value_small"),
               sum("exploded_data").alias("sum_exploded")))

# Force computation
print("Starting computation...")
result.explain(mode="extended")
result_count = result.count()
print(f"Result count: {result_count}")

# Show the distribution of data across original keys
print("Data distribution across original keys:")
df_large.groupBy("original_key").count().orderBy(col("count").desc()).show(10)

# Show the distribution of data across salted keys for the skewed key
print("Data distribution across salted keys for the skewed key:")
df_large.filter(col("original_key") == 0).groupBy("salted_key").count().orderBy(col("count").desc()).show(10)

# Show a sample of results
print("Sample results:")
result.orderBy(col("sum_value_large").desc()).show(10)