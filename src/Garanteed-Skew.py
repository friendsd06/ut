from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, lit, expr, sum, count, broadcast
import random

# Initialize Spark Session
spark = SparkSession.builder.appName("GuaranteedSkewEcommerceAnalysis").getOrCreate()

# Set a large number of shuffle partitions to make skew more apparent
spark.conf.set("spark.sql.shuffle.partitions", "1000")

# Generate skewed user data
def generate_skewed_users(num_users, num_influencers):
    return (spark.range(num_users)
            .withColumn("user_type", when(col("id") < num_influencers, "influencer").otherwise("regular"))
            .withColumnRenamed("id", "user_id"))

# Generate skewed product data
def generate_skewed_products(num_products, num_viral_products):
    return (spark.range(num_products)
            .withColumn("product_type", when(col("id") < num_viral_products, "viral").otherwise("regular"))
            .withColumnRenamed("id", "product_id"))

# Generate highly skewed order data
def generate_skewed_orders(num_users, num_products, num_orders, num_influencers, num_viral_products):
    def skewed_id(prefix, total, skewed):
        return when(
            (col("id") % 100 < 20),  # 20% of orders are for influencers/viral products
            expr(f"{prefix} % {skewed}")  # These orders are distributed among the influencers/viral products
        ).otherwise(
            expr(f"{prefix} % {total - skewed} + {skewed}")  # Other orders distributed among regular users/products
        )

    return (spark.range(num_orders)
            .withColumn("user_id", skewed_id("id", num_users, num_influencers))
            .withColumn("product_id", skewed_id("id", num_products, num_viral_products))
            .withColumn("quantity", when(col("id") % 100 < 20, lit(100)).otherwise(lit(1)))
            .withColumn("price", expr("10 + rand() * 990")))

# Generate datasets
num_users = 1000000
num_influencers = 100
num_products = 100000
num_viral_products = 50
num_orders = 10000000

users = generate_skewed_users(num_users, num_influencers)
products = generate_skewed_products(num_products, num_viral_products)
orders = generate_skewed_orders(num_users, num_products, num_orders, num_influencers, num_viral_products)

# Cache the smaller datasets to improve join performance
users.cache()
products.cache()

# Perform a skewed join and aggregation
result = (orders
          .join(users, "user_id")
          .join(products, "product_id")
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