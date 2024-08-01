from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr, when, sum, count, udf, explode, array, struct, to_json, lit
from pyspark.sql.types import ArrayType, StringType, IntegerType, StructType, StructField
import random

spark = SparkSession.builder.appName("SkewedTaskScenario").getOrCreate()

# Set a smaller number of shuffle partitions for this example
spark.conf.set("spark.sql.shuffle.partitions", "10")

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

# Generate skewed data with one very large key
def generate_skewed_data(num_records, num_keys, skewed_key):
    return (spark.range(num_records)
            .withColumn("key", when(col("id") % 10 == 0, lit(skewed_key))
                        .otherwise((col("id") % num_keys).cast("int")))
            .withColumn("value", when(col("key") == skewed_key, lit(1000000))
                        .otherwise(expr("rand() * 1000")))
            .withColumn("nested_data", complex_udf(col("value"))))

# Generate datasets
num_records = 100000  # 100,000 records
num_keys = 100  # 100 keys
skewed_key = 99  # The key that will have a lot more data

df_main = generate_skewed_data(num_records, num_keys, skewed_key)
df_lookup = spark.range(num_keys).withColumn("key", col("id").cast("int")).withColumn("info", lit("some_info"))

# Alias the DataFrames to avoid ambiguity
df_main = df_main.alias("main")
df_lookup = df_lookup.alias("lookup")

# Query that will result in one skewed task
result = (df_main.join(df_lookup, df_main.key == df_lookup.key)
          .withColumn("exploded_data", explode(df_main.nested_data))
          .groupBy(df_main.key, "exploded_data.category")
          .agg(
    count("*").alias("count"),
    sum(df_main.value).alias("sum_value"),
    sum("exploded_data.score").alias("sum_score")
)
          .withColumn("ratio", col("sum_value") / col("sum_score"))
          .filter(col("ratio") > 10)
          .orderBy(col("sum_value").desc()))

# Force computation
print("Starting computation...")
result.explain(mode="extended")
result_count = result.count()
print(f"Result count: {result_count}")

# Show a sample of results
print("Sample results:")
result.show(10, truncate=False)

# Show the distribution of data across keys
print("Data distribution across keys:")
df_main.groupBy("key").count().orderBy(col("count").desc()).show(5)