from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr, when, sum, count, udf, explode, array, struct, to_json
from pyspark.sql.types import ArrayType, StringType, IntegerType, StructType, StructField
import random

spark = SparkSession.builder.appName("ExtremeSkewAQEFailure").getOrCreate()

# Set a large number of shuffle partitions
spark.conf.set("spark.sql.shuffle.partitions", "10000")

# Disable broadcast joins to force shuffle joins
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")

# Complex UDF that AQE can't optimize
@udf(returnType=ArrayType(StructType([
    StructField("category", StringType()),
    StructField("score", IntegerType())
])))
def complex_udf(value):
    random.seed(value)
    return [{"category": random.choice(["A", "B", "C", "D", "E"]),
             "score": random.randint(1, 100)} for _ in range(random.randint(1, 10))]

# Generate extremely skewed data
def generate_skewed_data(num_records, num_keys, skew_factor):
    return (spark.range(num_records)
            .withColumn("key", (col("id") % num_keys).cast("int"))
            .withColumn("value", expr(f"pow(rand(), {skew_factor}) * 1000000"))
            .withColumn("nested_data", complex_udf(col("value"))))

# Generate datasets
num_records_large = 1000000000  # 1 billion
num_records_medium = 100000000  # 100 million
num_keys_large = 1000000  # 1 million
num_keys_medium = 10000  # 10 thousand
skew_factor = 10  # Extreme skew

df_large = generate_skewed_data(num_records_large, num_keys_large, skew_factor)
df_medium1 = generate_skewed_data(num_records_medium, num_keys_medium, skew_factor)
df_medium2 = generate_skewed_data(num_records_medium, num_keys_medium, skew_factor)

# Alias the DataFrames to avoid ambiguity
df_large = df_large.alias("large")
df_medium1 = df_medium1.alias("medium1")
df_medium2 = df_medium2.alias("medium2")

# Complex query with multiple challenges for AQE
result = (df_large.join(df_medium1, df_large.key == df_medium1.key)
          .join(df_medium2, df_large.key == df_medium2.key)
          .withColumn("exploded_data", explode(df_large.nested_data))  # Explicitly use df_large.nested_data
          .groupBy(df_large.key, "exploded_data.category")
          .agg(
    count("*").alias("count"),
    sum(df_large.value).alias("sum_value"),
    sum("exploded_data.score").alias("sum_score")
)
          .withColumn("ratio", col("sum_value") / col("sum_score"))
          .filter(col("ratio") > 100)
          .orderBy(col("sum_value").desc()))

# Force computation
print("Starting computation...")
result.explain(mode="extended")
result_count = result.count()
print(f"Result count: {result_count}")

# Show a sample of results
print("Sample results:")
result.show(10, truncate=False)