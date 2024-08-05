# Advanced Data Skew Handling in Apache Spark
# This notebook demonstrates techniques for handling data skew in Spark,
# including data generation, analysis, and optimization using salting.

# Import required libraries
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, rand, sha1, substring, concat, lit, when, current_date,
    current_timestamp, year, month, monotonically_increasing_id,
    count, avg, max, min, percentile_approx
)
from pyspark.sql.window import Window
import matplotlib.pyplot as plt
import time

# Initialize Spark session
spark = SparkSession.builder.appName("AdvancedDataSkewExample").getOrCreate()

# ========================
# Data Generation
# ========================

def generate_skewed_customers(num_customers, num_unique_ids):
    """Generate skewed customer data."""
    return (
        spark.range(num_customers)
            .withColumn("customer_id", (col("id") % num_unique_ids).cast("string"))
            .withColumn("name", concat(lit("Customer "), col("id").cast("string")))
            .withColumn("email", concat(col("name"), lit("@example.com")))
            .withColumn("registration_date", current_date())
            .withColumn("country", when(rand() < 0.8, "US").otherwise("Other"))
    )

def generate_skewed_transactions(num_transactions, num_unique_customer_ids, num_products):
    """Generate skewed transaction data with a long tail distribution."""
    return (
        spark.range(num_transactions)
            .withColumn("transaction_id", monotonically_increasing_id().cast("string"))
            .withColumn("customer_id", (
                (col("id") % num_unique_customer_ids) +
                when(rand() < 0.2, lit(num_unique_customer_ids)).otherwise(lit(0))
        ).cast("string"))
            .withColumn("product_id", (rand() * num_products).cast("int").cast("string"))
            .withColumn("amount", (rand() * 1000).cast("decimal(10,2)"))
            .withColumn("transaction_date", current_timestamp())
            .withColumn("year", year(col("transaction_date")))
            .withColumn("month", month(col("transaction_date")))
    )

# Generate sample data
num_customers = 1000000
num_transactions = 10000000
num_unique_customer_ids = 1000
num_products = 10000

print("Generating skewed customer and transaction data...")
skewed_customers = generate_skewed_customers(num_customers, num_unique_customer_ids)
skewed_transactions = generate_skewed_transactions(num_transactions, num_unique_customer_ids, num_products)

# Display sample data
print("\nSample of skewed customer data:")
skewed_customers.show(5)
print("\nSample of skewed transaction data:")
skewed_transactions.show(5)

# ========================
# Data Analysis
# ========================

def analyze_distribution(df, group_col, count_col, top_n=10):
    """Analyze the distribution of a DataFrame."""
    distribution = df.groupBy(group_col).agg(count(count_col).alias("count"))

    # Calculate statistics
    stats = distribution.agg(
        avg("count").alias("avg_count"),
        max("count").alias("max_count"),
        min("count").alias("min_count"),
        percentile_approx("count", 0.5).alias("median_count"),
        percentile_approx("count", 0.95).alias("95th_percentile_count")
    )

    # Show top N results
    print(f"\nTop {top_n} {group_col} by {count_col} count:")
    distribution.orderBy(col("count").desc()).show(top_n)

    # Show statistics
    print("\nDistribution statistics:")
    stats.show()

    return distribution

print("Analyzing customer distribution in transactions:")
customer_distribution = analyze_distribution(skewed_transactions, "customer_id", "transaction_id")

# ========================
# Join Operation (Skewed Data)
# ========================

print("\nPerforming join operation on skewed data...")
skewed_result = skewed_transactions.join(skewed_customers, "customer_id")
print("Join completed. Skewed result schema:")
skewed_result.printSchema()

# ========================
# Salting Implementation
# ========================

def salt_dataframe(df, id_col, num_salt_values):
    """Apply salting to a DataFrame to mitigate skew."""
    return (
        df.withColumn("salt", (rand() * num_salt_values).cast("int"))
            .withColumn(f"salted_{id_col}", concat(col(id_col), lit("_"), col("salt").cast("string")))
    )

num_salt_values = 10

print("\nApplying salting to mitigate skew...")
salted_customers = salt_dataframe(skewed_customers, "customer_id", num_salt_values)
salted_transactions = salt_dataframe(skewed_transactions, "customer_id", num_salt_values)

# Display sample of salted data
print("\nSample of salted customer data:")
salted_customers.show(5)
print("\nSample of salted transaction data:")
salted_transactions.show(5)

# Perform the join operation with salted keys
print("\nPerforming join operation on salted data...")
salted_result = salted_transactions.join(salted_customers, "salted_customer_id")
print("Join completed. Salted result schema:")
salted_result.printSchema()

# ========================
# Post-Salting Analysis
# ========================

print("\nAnalyzing distribution after salting:")
salted_distribution = analyze_distribution(salted_result, "customer_id", "transaction_id")

# ========================
# Visualization
# ========================

def plot_distribution(df, x_col, y_col, title, x_label, y_label, top_n=50):
    """Plot the distribution of a DataFrame."""
    data = df.orderBy(col(y_col).desc()).limit(top_n).toPandas()
    plt.figure(figsize=(12, 6))
    plt.bar(data[x_col].astype(str), data[y_col])
    plt.title(title)
    plt.xlabel(x_label)
    plt.ylabel(y_label)
    plt.xticks(rotation=90)
    plt.tight_layout()
    plt.show()

print("\nGenerating distribution plots...")
plot_distribution(customer_distribution, "customer_id", "count",
                  "Customer Transaction Distribution (Before Salting)",
                  "Customer ID", "Number of Transactions")

plot_distribution(salted_distribution, "customer_id", "count",
                  "Customer Transaction Distribution (After Salting)",
                  "Customer ID", "Number of Transactions")

# ========================
# Performance Comparison
# ========================

def benchmark_join(df1, df2, join_col):
    """Benchmark the join operation."""
    start_time = time.time()
    result = df1.join(df2, join_col).count()
    end_time = time.time()
    return end_time - start_time

print("\nBenchmarking join operations...")
skewed_join_time = benchmark_join(skewed_transactions, skewed_customers, "customer_id")
salted_join_time = benchmark_join(salted_transactions, salted_customers, "salted_customer_id")

print(f"Skewed join execution time: {skewed_join_time:.2f} seconds")
print(f"Salted join execution time: {salted_join_time:.2f} seconds")
print(f"Performance improvement: {(skewed_join_time - salted_join_time) / skewed_join_time * 100:.2f}%")

# Clean up
spark.stop()