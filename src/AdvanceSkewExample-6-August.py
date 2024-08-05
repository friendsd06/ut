from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, rand, lit, count, date_add, current_date, array, explode, sum
import matplotlib.pyplot as plt
import time

# Initialize Spark session
spark = SparkSession.builder.appName("HighlySkewedNestedJoinExample").getOrCreate()

# Set a large number of shuffle partitions to make skew more apparent
spark.conf.set("spark.sql.shuffle.partitions", "200")

# Disable broadcast joins to force shuffle joins
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")

# Generate highly skewed transaction data with nested column
def generate_transactions(num_transactions):
    return (spark.range(num_transactions)
            .withColumn("transaction_id", col("id").cast("string"))
            .withColumn("customer_id",
                        when(rand() < 0.90, lit("customer_1"))
                        .otherwise((rand() * 999999 + 2).cast("int").cast("string")))
            .withColumn("amount", (rand() * 1000).cast("decimal(10,2)"))
            .withColumn("product_id", (rand() * 100).cast("int").cast("string"))
            .withColumn("transaction_date", date_add(current_date(), -(rand() * 365).cast("int")))
            .withColumn("store_id", (rand() * 50).cast("int").cast("string"))
            .withColumn("nested_data", array([rand() * 10 for _ in range(10)])))

# Generate skewed customer data
def generate_customers(num_customers):
    return (spark.range(num_customers)
            .withColumn("customer_id",
                        when(col("id") == 0, lit("customer_1"))
                        .otherwise(concat(lit("customer_"), col("id").cast("string"))))
            .withColumn("name", concat(lit("Customer "), col("id").cast("string")))
            .withColumn("category", when(col("customer_id") == "customer_1", lit("Premium"))
                        .otherwise(lit("Regular")))
            .withColumn("value", when(col("customer_id") == "customer_1", lit(1000000))
                        .otherwise(rand() * 100)))

# Create transaction data
num_transactions = 50000000  # 50 million
transactions = generate_transactions(num_transactions)

# Create customer data
num_customers = 1000000  # 1 million
customer_data = generate_customers(num_customers)

# Show sample data
print("Sample of transaction data:")
transactions.show(5, truncate=False)
print("\nSample of customer data:")
customer_data.show(5)

# Analyze distribution of transactions
distribution = transactions.groupBy("customer_id").count().orderBy("count", ascending=False)
print("\nCustomer transaction distribution:")
distribution.show(10)

# Calculate percentage for top customer
top_customer_count = distribution.first()['count']
top_customer_percentage = (top_customer_count / num_transactions) * 100
print(f"Top customer percentage: {top_customer_percentage:.2f}%")

# Perform skewed join with additional operations
print("\nPerforming skewed join with additional operations...")
result = (transactions.join(customer_data, "customer_id")
          .withColumn("exploded_data", explode(col("nested_data")))
          .groupBy("customer_id", "category")
          .agg(count("*").alias("count"),
               sum("amount").alias("sum_amount"),
               sum("value").alias("sum_value"),
               sum("exploded_data").alias("sum_exploded")))

# Analyze join result
print("Join result distribution:")
join_distribution = result.groupBy("category").agg(sum("count").alias("total_count"),
                                                   sum("sum_amount").alias("total_amount"),
                                                   sum("sum_value").alias("total_value"),
                                                   sum("sum_exploded").alias("total_exploded"))
join_distribution.show()

# Force computation and measure time
start_time = time.time()
result_count = result.count()
end_time = time.time()
join_time = end_time - start_time

print(f"Join and aggregation execution time: {join_time:.2f} seconds")
print(f"Result count: {result_count}")

# Show a sample of the result
print("\nSample of the result:")
result.orderBy(col("sum_amount").desc()).show(10)

# Clean up
spark.stop()