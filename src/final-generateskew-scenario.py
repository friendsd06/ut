from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, avg, count, dense_rank, ntile, when, expr
from pyspark.sql.window import Window
import time

# Initialize Spark session
spark = SparkSession.builder.appName("ComplexSkewScenario").getOrCreate()

# Disable adaptive query execution to make skew more apparent
spark.conf.set("spark.sql.adaptive.enabled", "false")

# Read data from S3
customers_df = spark.read.csv("s3://your-bucket/customers", header=True, inferSchema=True)
loans_df = spark.read.csv("s3://your-bucket/loans", header=True, inferSchema=True)

# Complex query to generate skew
def complex_skew_scenario():
    # Join loans with customers
    joined_df = loans_df.join(customers_df, "customer_id")

    # Window for customer ranking
    customer_window = Window.partitionBy("type").orderBy(col("loan_amount").desc())

    # Window for percentile calculation
    percentile_window = Window.partitionBy("type")

    # Complex aggregation and window functions
    result_df = joined_df.groupBy("customer_id", "type", "name") \
        .agg(
        count("loan_id").alias("loan_count"),
        sum("loan_amount").alias("total_loan_amount"),
        avg("loan_amount").alias("avg_loan_amount")
    ) \
        .withColumn("customer_rank", dense_rank().over(customer_window)) \
        .withColumn("percentile", ntile(100).over(percentile_window)) \
        .withColumn("risk_category",
                    when(col("percentile") <= 20, "Very High Risk")
                    .when(col("percentile") <= 40, "High Risk")
                    .when(col("percentile") <= 60, "Medium Risk")
                    .when(col("percentile") <= 80, "Low Risk")
                    .otherwise("Very Low Risk")
                    )

    # Self-join to compare customers within the same risk category
    skewed_result = result_df.alias("a").join(
        result_df.alias("b"),
        (col("a.risk_category") == col("b.risk_category")) & (col("a.customer_id") != col("b.customer_id"))
    )

    # Aggregate to find average difference in loan amounts within risk categories
    final_result = skewed_result.groupBy("a.risk_category") \
        .agg(
        avg(expr("abs(a.total_loan_amount - b.total_loan_amount)")).alias("avg_loan_amount_diff"),
        count("*").alias("comparison_count")
    ) \
        .orderBy("avg_loan_amount_diff", ascending=False)

    return final_result

# Execute the complex query and measure time
start_time = time.time()
result = complex_skew_scenario()
result.show(truncate=False)
end_time = time.time()

print(f"Query execution time: {end_time - start_time} seconds")

# Show the query plan to visualize the skew
result.explain(extended=True)

# Additional analysis to show skew
print("\nData distribution across executors:")
result.rdd.mapPartitionsWithIndex(lambda idx, iter: [(idx, len(list(iter)))]).toDF(["partition", "count"]).show()

# Clean up
spark.stop()