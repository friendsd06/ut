from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr, when, sum, count, broadcast
import random

# Initialize Spark Session
spark = SparkSession.builder.appName("SkewedEcommerceAnalysis").getOrCreate()

# Set a large number of shuffle partitions to make skew more apparent
spark.conf.set("spark.sql.shuffle.partitions", "1000")

# Generate skewed user data
def generate_skewed_users(num_users, num_influencers):
    return (spark.range(num_users)
            .withColumn("user_type", when(col("id") < num_influencers, "influencer").otherwise("regular"))
            .withColumn("activity_multiplier", when(col("user_type") == "influencer", expr("1000 + rand() * 9000")).otherwise(expr("1 + rand() * 9")))
            .withColumnRenamed("id", "user_id"))

# Generate skewed product data
def generate_skewed_products(num_products, num_viral_products):
    return (spark.range(num_products)
            .withColumn("product_type", when(col("id") < num_viral_products, "viral").otherwise("regular"))
            .withColumn("popularity_score", when(col("product_type") == "viral", expr("1000 + rand() * 9000")).otherwise(expr("1 + rand() * 99")))
            .withColumnRenamed("id", "product_id"))

# Generate highly skewed order data
def generate_skewed_orders(num_users, num_products, num_orders):
    return (spark.range(num_orders)
            .withColumn("user_id", expr(f"cast(pow(rand(), 2) * {num_users} as long)"))
            .withColumn("product_id", expr(f"cast(pow(rand(), 2) * {num_products} as long)"))
            .withColumn("quantity", expr("1 + rand() * 10"))
            .withColumn("price", expr("10 + rand() * 990")))

# Generate datasets
num_users = 1000000
num_influencers = 100
num_products = 100000
num_viral_products = 50
num_orders = 100000000

users = generate_skewed_users(num_users, num_influencers)
products = generate_skewed_products(num_products, num_viral_products)
orders = generate_skewed_orders(num_users, num_products, num_orders)

# Cache the smaller datasets to improve join performance
users.cache()
products.cache()

# Perform a skewed join and aggregation
result = (orders
          .join(broadcast(users), "user_id")  # Broadcast the smaller users table
          .join(broadcast(products), "product_id")  # Broadcast the smaller products table
          .groupBy("user_type", "product_type")
          .agg(
    count("*").alias("order_count"),
    sum("quantity").alias("total_quantity"),
    sum(col("quantity") * col("price")).alias("total_revenue")
)
          .orderBy(col("total_revenue").desc()))

# Show execution plan
print("Execution Plan:")
result.explain(mode="extended")

# Show results
print("\nResults:")
result.show()

# Collect statistics to demonstrate skew
user_order_counts = orders.groupBy("user_id").count().orderBy(col("count").desc())
product_order_counts = orders.groupBy("product_id").count().orderBy(col("count").desc())

print("\nTop 5 Users by Order Count:")
user_order_counts.show(5)

print("\nTop 5 Products by Order Count:")
product_order_counts.show(5)

# Clean up
spark.catalog.clearCache()