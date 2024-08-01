from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr, when, sum, count, udf, explode, array, struct, lit, concat
from pyspark.sql.types import ArrayType, StringType, IntegerType, StructType, StructField
import random

spark = SparkSession.builder.appName("ComplexSkewScenario").getOrCreate()

# Set a larger number of shuffle partitions
spark.conf.set("spark.sql.shuffle.partitions", "200")

# Disable broadcast joins to force shuffle joins
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")

# Complex UDF that AQE can't optimize
@udf(returnType=ArrayType(StructType([
    StructField("category", StringType()),
    StructField("score", IntegerType()),
    StructField("subcategories", ArrayType(StringType()))
])))
def complex_udf(value):
    random.seed(value)
    return [{"category": random.choice(["A", "B", "C", "D", "E"]),
             "score": random.randint(1, 100),
             "subcategories": [random.choice(["X", "Y", "Z"]) for _ in range(random.randint(1, 5))]}
            for _ in range(random.randint(1, 10))]

# Generate skewed data
def generate_skewed_data(num_records, num_partitions, skew_factor):
    return (spark.range(num_records)
            .withColumn("key1", (col("id") % num_partitions).cast("int"))
            .withColumn("key2", ((col("id") + 1) % num_partitions).cast("int"))
            .withColumn("value", expr(f"pow(rand(), {skew_factor}) * 1000000"))
            .withColumn("nested_data", complex_udf(col("value"))))

# Generate datasets
num_records_large = 10000000  # 10 million
num_records_medium = 1000000  # 1 million
num_partitions = 200
skew_factor = 5

df_large = generate_skewed_data(num_records_large, num_partitions, skew_factor)
df_medium1 = generate_skewed_data(num_records_medium, num_partitions, skew_factor)
df_medium2 = generate_skewed_data(num_records_medium, num_partitions, skew_factor)

# Additional dataset to join
df_additional = spark.range(1000).withColumn("key", (col("id") % num_partitions).cast("int"))

# Alias the DataFrames
df_large = df_large.alias("large")
df_medium1 = df_medium1.alias("medium1")
df_medium2 = df_medium2.alias("medium2")
df_additional = df_additional.alias("additional")

# Complex query with multiple challenges for AQE
result = (df_large.join(df_medium1, df_large.key1 == df_medium1.key1)
          .join(df_medium2, df_large.key2 == df_medium2.key1)
          .join(df_additional, df_large.key1 == df_additional.key)
          .withColumn("exploded_data", explode(df_large.nested_data))
          .withColumn("further_exploded", explode("exploded_data.subcategories"))
          .groupBy(df_large.key1, df_large.key2, "exploded_data.category", "further_exploded")
          .agg(
    count("*").alias("count"),
    sum(df_large.value + df_medium1.value + df_medium2.value).alias("sum_value"),
    sum("exploded_data.score").alias("sum_score")
)
          .withColumn("ratio", col("sum_value") / col("sum_score"))
          .withColumn("complex_key", concat(col("key1"), lit("_"), col("key2"), lit("_"), col("category"), lit("_"), col("further_exploded")))
          .filter(col("ratio") > 1000)
          .orderBy(col("sum_value").desc()))

# Force computation
print("Starting computation...")
result.explain(mode="extended")
result_count = result.count()
print(f"Result count: {result_count}")

# Show a sample of results
print("Sample results:")
result.show(10, truncate=False)