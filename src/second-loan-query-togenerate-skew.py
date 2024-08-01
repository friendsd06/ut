# Databricks notebook source

# COMMAND ----------
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

spark = SparkSession.builder.getOrCreate()

# COMMAND ----------
# Define schemas (same as before)
loan_schema = StructType([
    StructField("loan_id", LongType(), False),
    StructField("customer_id", LongType(), False),
    StructField("loan_amount", DoubleType(), False),
    # ... (include all 30 fields as defined earlier)
])

customer_schema = StructType([
    StructField("customer_id", LongType(), False),
    StructField("first_name", StringType(), False),
    StructField("last_name", StringType(), False),
    # ... (include all 12 fields as defined earlier)
])

# COMMAND ----------
# Read data from S3
s3_bucket = "your-s3-bucket-name"
customer_path = f"s3a://{s3_bucket}/customer_data.csv"
loan_path = f"s3a://{s3_bucket}/loan_data.csv"

customer_df = spark.read.csv(customer_path, header=True, schema=customer_schema)
loan_df = spark.read.csv(loan_path, header=True, schema=loan_schema)

# COMMAND ----------
# Generate skew scenario
def generate_skew_scenario():
    # Join loans with customers
    joined_df = loan_df.join(customer_df, "customer_id")

    # Group by customer and calculate total loan amount
    customer_loan_totals = joined_df.groupBy("customer_id", "first_name", "last_name") \
        .agg(sum("loan_amount").alias("total_loan_amount"),
             count("loan_id").alias("loan_count"))

    # Find the customer with the highest loan count (this should be our skewed customer)
    skewed_customer = customer_loan_totals.orderBy(col("loan_count").desc()).first()
    skewed_customer_id = skewed_customer.customer_id

    print(f"Skewed customer ID: {skewed_customer_id}")
    print(f"Skewed customer loan count: {skewed_customer.loan_count}")
    print(f"Skewed customer total loan amount: ${skewed_customer.total_loan_amount:,.2f}")

    # Calculate percentages
    total_loans = loan_df.count()
    skewed_customer_percentage = (skewed_customer.loan_count / total_loans) * 100

    print(f"Percentage of loans belonging to skewed customer: {skewed_customer_percentage:.2f}%")

    # Show distribution of loans
    print("\nLoan distribution:")
    customer_loan_totals.orderBy(col("loan_count").desc()).show(10)

    # Perform a query that will generate skew
    skewed_query = joined_df.groupBy("customer_id", "first_name", "last_name") \
        .agg(sum("loan_amount").alias("total_loan_amount"),
             avg("interest_rate").alias("avg_interest_rate"),
             max("loan_term").alias("max_loan_term"),
             min("origination_date").alias("earliest_loan_date"),
             max("maturity_date").alias("latest_maturity_date"),
             sum(when(col("loan_status") == "Active", 1).otherwise(0)).alias("active_loans"),
             sum(when(col("loan_status") == "Defaulted", 1).otherwise(0)).alias("defaulted_loans"))

    print("\nSkewed query result (showing top 10 customers by total loan amount):")
    skewed_query.orderBy(col("total_loan_amount").desc()).show(10)

    return skewed_query

# COMMAND ----------
# Run the skew scenario
skewed_result = generate_skew_scenario()

# COMMAND ----------
# Optionally, you can write the skewed result back to S3 for further analysis
skewed_result_path = f"s3a://{s3_bucket}/skewed_result.csv"
skewed_result.write.csv(skewed_result_path, header=True, mode="overwrite")

print(f"Skewed result written to: {skewed_result_path}")

