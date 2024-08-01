from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, arrays_zip, when, sum, avg, stddev, percentile_approx, udf
from pyspark.sql.types import ArrayType, StringType, IntegerType
import random

spark = SparkSession.builder.appName("UltraComplexAnalysis").getOrCreate()

# Custom UDF for complex text analysis (simulating NLP)
@udf(returnType=ArrayType(StringType()))
def complex_text_analysis(text):
    # Simulate complex NLP operation
    return random.sample(['positive', 'negative', 'neutral'] * 3, random.randint(1, 9))

# Simulating massive datasets
users = spark.range(100000000).toDF("user_id") \
    .withColumn("region", (col("user_id") % 1000).cast("string")) \
    .withColumn("preferences", arrays_zip(
    spark.range(10).select((col("id") % 5).alias("category")),
    spark.range(10).select((col("id") * 0.1).alias("score"))
))

products = spark.range(10000000).toDF("product_id") \
    .withColumn("category", (col("product_id") % 1000).cast("string")) \
    .withColumn("subcategory", (col("product_id") % 10000).cast("string")) \
    .withColumn("attributes", arrays_zip(
    spark.range(20).select(col("id").alias("attr_id")),
    spark.range(20).select((col("id") * 0.5).alias("attr_value"))
))

orders = spark.range(1000000000).toDF("order_id") \
    .withColumn("user_id", (col("order_id") % 100000000).cast("long")) \
    .withColumn("product_id", (col("order_id") % 10000000).cast("long")) \
    .withColumn("timestamp", col("order_id") + 1600000000) \
    .withColumn("order_items", arrays_zip(
    spark.range(5).select(col("id").alias("item_id")),
    spark.range(5).select((col("id") + 1).alias("quantity"))
))

social_interactions = spark.range(5000000000).toDF("interaction_id") \
    .withColumn("user_id", (col("interaction_id") % 100000000).cast("long")) \
    .withColumn("product_id", (col("interaction_id") % 10000000).cast("long")) \
    .withColumn("interaction_type", (col("interaction_id") % 5).cast("string")) \
    .withColumn("timestamp", col("interaction_id") + 1600000000) \
    .withColumn("text_content", (col("interaction_id") % 1000).cast("string"))

events = spark.range(50000000).toDF("event_id") \
    .withColumn("event_type", (col("event_id") % 100).cast("string")) \
    .withColumn("timestamp", col("event_id") + 1600000000) \
    .withColumn("affected_categories", arrays_zip(
    spark.range(5).select((col("id") % 1000).alias("category")),
    spark.range(5).select((col("id") * 0.1).alias("impact_score"))
))

# Ultra-complex analysis
result = users.join(orders, "user_id") \
    .join(products, "product_id") \
    .join(social_interactions, ["user_id", "product_id"]) \
    .join(events, orders.timestamp.between(events.timestamp, events.timestamp + 86400)) \
    .select(
    "user_id", "product_id", "region", "category", "subcategory",
    explode("preferences").alias("pref"),
    explode("attributes").alias("attr"),
    explode("order_items").alias("order_item"),
    "interaction_type", "text_content", "event_type"
)
.withColumn("sentiment", complex_text_analysis(col("text_content")))
.select(
    col("region"),
    col("category"),
    col("subcategory"),
    col("pref.category").alias("user_pref_category"),
    col("pref.score").alias("user_pref_score"),
    col("attr.attr_id").alias("product_attr_id"),
    col("attr.attr_value").alias("product_attr_value"),
    col("order_item.quantity").alias("order_quantity"),
    col("interaction_type"),
    explode("sentiment").alias("sentiment"),
    col("event_type")
)
.groupBy("region", "category", "subcategory", "user_pref_category", "product_attr_id", "interaction_type", "sentiment", "event_type")
.agg(
    avg("user_pref_score").alias("avg_user_pref_score"),
    stddev("user_pref_score").alias("stddev_user_pref_score"),
    sum("order_quantity").alias("total_quantity"),
    avg("product_attr_value").alias("avg_product_attr_value"),
    percentile_approx("product_attr_value", 0.5).alias("median_product_attr_value")
)
.where(col("total_quantity") > 100)

# Execute and explain
result.explain(mode="extended")
result.show(truncate=False)