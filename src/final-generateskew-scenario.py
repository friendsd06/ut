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

def simple_skew_scenario():
    # Join loans with customers
    joined_df = loans_df.join(customers_df, "customer_id")

    # Aggregate loans by customer
    customer_loans = joined_df.groupBy("customer_id", "type", "name") \
        .agg(
        count("loan_id").alias("loan_count"),
        sum("loan_amount").alias("total_loan_amount")
    )

    # Create a high cardinality column based on loan amount
    # This will force more shuffling and make skew more apparent
    customer_loans = customer_loans.withColumn(
        "amount_category",
        expr("cast(total_loan_amount / 1000 as int)")
    )

    # Perform a self-join on amount_category
    # This operation will amplify the skew
    skewed_result = customer_loans.alias("a").join(
        customer_loans.alias("b"),
        (col("a.amount_category") == col("b.amount_category")) &
        (col("a.customer_id") != col("b.customer_id"))
    )

    # Aggregate the results
    final_result = skewed_result.groupBy("a.type") \
        .agg(
        count("*").alias("comparison_count"),
        sum("a.total_loan_amount").alias("total_amount"),
        avg("a.loan_count").alias("avg_loan_count")
    ) \
        .orderBy("comparison_count", ascending=False)

    return final_result

# Execute the query and measure time
start_time = time.time()
result = simple_skew_scenario()
result.show(truncate=False)
end_time = time.time()

print(f"Query execution time: {end_time - start_time} seconds")