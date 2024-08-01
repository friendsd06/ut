from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, avg, when

spark = SparkSession.builder.appName("ComplexSkewJoin").getOrCreate()

# Simulating data loading (you would typically read from actual data sources)
users = spark.range(10000000).toDF("user_id").withColumn("age_group", (col("user_id") % 5).cast("string"))
orders = spark.range(100000000).toDF("order_id").withColumn("user_id", (col("order_id") % 10000000).cast("long")).withColumn("product_id", (col("order_id") % 1000000).cast("long"))
products = spark.range(1000000).toDF("product_id").withColumn("category", (col("product_id") % 10).cast("string"))
reviews = spark.range(500000000).toDF("review_id").withColumn("product_id", (col("review_id") % 1000000).cast("long")).withColumn("user_id", (col("review_id") % 10000000).cast("long")).withColumn("rating", (col("review_id") % 5 + 1).cast("int"))

# Complex join operation
result = users.join(orders, "user_id") \
    .join(products, "product_id") \
    .join(reviews, ["user_id", "product_id"]) \
    .groupBy("age_group", "category") \
    .agg(
    avg("rating").alias("avg_rating"),
    sum(when(col("rating") >= 4, 1).otherwise(0)).alias("high_ratings"),
    sum(when(col("rating") <= 2, 1).otherwise(0)).alias("low_ratings")
)

# Execute and show results
result.explain(mode="extended")
result.show()