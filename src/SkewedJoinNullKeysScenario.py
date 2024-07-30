from pyspark.sql import SparkSession
from dbldatagen import DataGenerator, fakergen
from pyspark.sql.functions import col, expr, when, count, isnull, isnotnull
import time
import matplotlib.pyplot as plt
import seaborn as sns

# Initialize Spark session
spark = SparkSession.builder.appName("SkewedJoinNullKeysScenario").getOrCreate()

# Set configurations to make the skew more apparent
spark.conf.set("spark.sql.shuffle.partitions", "200")
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")  # Disable broadcast joins
spark.conf.set("spark.sql.adaptive.enabled", "false")  # Disable adaptive query execution for this example

# Generate a large dataset with skewed join keys (including many nulls)
orders_gen = (DataGenerator(spark, name="orders", rowcount=10_000_000, partitions=50)
              .withColumn("order_id", expr("uuid()"))
              .withColumn("customer_id", expr("""
        case
            when rand() < 0.3 then null  # 30% null values
            when rand() < 0.6 then array('CUST1', 'CUST2', 'CUST3', 'CUST4', 'CUST5')[int(rand() * 5)]  # 30% skewed to 5 customers
            else concat('CUST', cast(rand() * 995 + 6 as int))  # Rest distributed among 995 customers
        end
    """))
              .withColumn("order_date", expr("date_sub(current_date(), int(rand() * 365))"))
              .withColumn("total_amount", expr("rand() * 1000"))
              )

# Generate customer dataset
customers_gen = (DataGenerator(spark, name="customers", rowcount=1000, partitions=5)
                 .withColumn("customer_id", expr("concat('CUST', id)"))
                 .withColumn("name", fakergen("name"))
                 .withColumn("email", fakergen("email"))
                 .withColumn("registration_date", expr("date_sub(current_date(), int(rand() * 1000))"))
                 )

# Build the datasets
orders_df = orders_gen.build()
customers_df = customers_gen.build()

print("Datasets generated. Sample data:")
print("\nOrders:")
orders_df.show(10, truncate=False)
print("\nCustomers:")
customers_df.show(10, truncate=False)

# Analyze distribution of customer_id in orders
print("\nAnalyzing customer_id distribution in orders:")
customer_distribution = orders_df.groupBy("customer_id").agg(count("*").alias("order_count")).orderBy(col("order_count").desc())
customer_distribution.show(20, truncate=False)

print("\nNull vs Non-null customer_id in orders:")
orders_df.groupBy(isnull("customer_id")).agg(count("*").alias("count")).show()

# Visualize customer_id distribution
plt.figure(figsize=(15, 10))
customer_dist_data = customer_distribution.filter(col("customer_id").isNotNull()).limit(50).toPandas()
sns.barplot(x='customer_id', y='order_count', data=customer_dist_data)
plt.title('Order Count by Customer ID (Top 50 Non-null)')
plt.xlabel('Customer ID')
plt.ylabel('Order Count')
plt.xticks(rotation=90)
plt.tight_layout()
plt.savefig('customer_distribution.png')
plt.close()

# Perform the skewed join
print("\nPerforming join operation...")
start_time = time.time()

joined_df = orders_df.join(customers_df, "customer_id", "left")

# Force the action to execute
result_count = joined_df.count()
end_time = time.time()

print(f"\nJoin operation completed. Result count: {result_count}")
print(f"Execution time: {end_time - start_time:.2f} seconds")

print("\nSample of joined data:")
joined_df.show(20, truncate=False)

# Analyze the join results
print("\nAnalyzing join results:")
join_analysis = joined_df.groupBy(
    when(isnull("customer_id"), "Null")
        .when(col("customer_id").isin("CUST1", "CUST2", "CUST3", "CUST4", "CUST5"), "Top 5 Customers")
        .otherwise("Other Customers")
).agg(count("*").alias("row_count"))

join_analysis.show(truncate=False)

# Visualize join results
plt.figure(figsize=(10, 6))
join_analysis_data = join_analysis.toPandas()
sns.barplot(x='case', y='row_count', data=join_analysis_data)
plt.title('Join Results Distribution')
plt.xlabel('Customer Category')
plt.ylabel('Row Count')
plt.tight_layout()
plt.savefig('join_results_distribution.png')
plt.close()

print("\nNote: To observe the impact of the skewed join, check the Spark UI for uneven task durations in the join stage.")

# Clean up
spark.catalog.clearCache()