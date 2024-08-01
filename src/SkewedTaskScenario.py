from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr, when, sum, count, udf, explode, array, struct, lit
from pyspark.sql.types import ArrayType, StringType, IntegerType, StructType, StructField
import random

spark = SparkSession.builder.appName("GuaranteedSkewScenario").getOrCreate()

# Set a small number of shuffle partitions to make skew more apparent
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

# Generate extremely skewed data
def generate_skewed_data(num_records, num_partitions):
    return (spark.range(num_records)
            .withColumn("key", when(col("id") % num_partitions == 0, lit(0))  # Skewed key
                        .otherwise((col("id") % num_partitions).cast("int")))
            .withColumn("value", when(col("key") == 0, lit(1000000))  # Large value for skewed key
                        .otherwise(expr("rand() * 1000")))
            .withColumn("nested_data", complex_udf(col("value"))))

# Generate datasets
num_records = 1000000  # 1 million records
num_partitions = 10

df_main = generate_skewed_data(num_records, num_partitions)
df_lookup = spark.range(num_partitions).withColumn("key", col("id").cast("int")).withColumn("info", lit("some_info"))

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
df_main.groupBy("key").count().orderBy(col("count").desc()).show()