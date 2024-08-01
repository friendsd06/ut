# Databricks notebook source

# COMMAND ----------
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import random

spark = SparkSession.builder.getOrCreate()

# COMMAND ----------
# Define schema for loan dataset (30 attributes)
loan_schema = StructType([
    StructField("loan_id", LongType(), False),
    StructField("customer_id", LongType(), False),
    StructField("loan_amount", DoubleType(), False),
    StructField("interest_rate", DoubleType(), False),
    StructField("loan_term", IntegerType(), False),
    StructField("loan_type", StringType(), False),
    StructField("loan_status", StringType(), False),
    StructField("origination_date", DateType(), False),
    StructField("maturity_date", DateType(), False),
    StructField("payment_frequency", StringType(), False),
    StructField("monthly_payment", DoubleType(), False),
    StructField("total_payments_made", IntegerType(), False),
    StructField("remaining_balance", DoubleType(), False),
    StructField("last_payment_date", DateType(), False),
    StructField("next_payment_date", DateType(), False),
    StructField("days_past_due", IntegerType(), False),
    StructField("times_30_days_late", IntegerType(), False),
    StructField("times_60_days_late", IntegerType(), False),
    StructField("times_90_days_late", IntegerType(), False),
    StructField("collateral_type", StringType(), False),
    StructField("collateral_value", DoubleType(), False),
    StructField("loan_to_value_ratio", DoubleType(), False),
    StructField("debt_to_income_ratio", DoubleType(), False),
    StructField("credit_score_at_origination", IntegerType(), False),
    StructField("current_credit_score", IntegerType(), False),
    StructField("employment_status", StringType(), False),
    StructField("income_verification", StringType(), False),
    StructField("loan_purpose", StringType(), False),
    StructField("branch_id", IntegerType(), False),
    StructField("underwriter_id", IntegerType(), False)
])

# Define schema for customer dataset (12 attributes)
customer_schema = StructType([
    StructField("customer_id", LongType(), False),
    StructField("first_name", StringType(), False),
    StructField("last_name", StringType(), False),
    StructField("email", StringType(), False),
    StructField("phone_number", StringType(), False),
    StructField("address", StringType(), False),
    StructField("city", StringType(), False),
    StructField("state", StringType(), False),
    StructField("zip_code", StringType(), False),
    StructField("date_of_birth", DateType(), False),
    StructField("annual_income", DoubleType(), False),
    StructField("credit_score", IntegerType(), False)
])

# COMMAND ----------
# Generate customer data
def generate_customer_data(num_customers):
    return (spark.range(0, num_customers)
            .withColumn("customer_id", col("id"))
            .withColumn("first_name", expr("concat('FirstName_', id)"))
            .withColumn("last_name", expr("concat('LastName_', id)"))
            .withColumn("email", expr("concat('user', id, '@example.com')"))
            .withColumn("phone_number", expr("concat('+1', lpad(cast(rand() * 9999999999 as int), 10, '0'))"))
            .withColumn("address", expr("concat(cast(rand() * 9999 as int), ' Main St')"))
            .withColumn("city", expr("concat('City_', cast(rand() * 100 as int))"))
            .withColumn("state", expr("concat('State_', cast(rand() * 50 as int))"))
            .withColumn("zip_code", expr("lpad(cast(rand() * 99999 as int), 5, '0')"))
            .withColumn("date_of_birth", expr("date_sub(current_date(), cast(rand() * 365 * 70 as int))"))
            .withColumn("annual_income", expr("rand() * 200000"))
            .withColumn("credit_score", expr("300 + cast(rand() * 550 as int)"))
            .select(*[column for column in customer_schema.fieldNames()])
            )

# COMMAND ----------
# Generate loan data with 97% belonging to one customer
def generate_loan_data(num_loans, skewed_customer_id, other_customer_ids):
    skewed_loans = int(num_loans * 0.97)
    other_loans = num_loans - skewed_loans

    skewed_df = (spark.range(0, skewed_loans)
                 .withColumn("customer_id", lit(skewed_customer_id)))

    other_df = (spark.range(skewed_loans, num_loans)
                .withColumn("customer_id", expr(f"array({','.join(map(str, other_customer_ids))})[cast(rand() * {len(other_customer_ids)} as int)]")))

    return (skewed_df.union(other_df)
            .withColumn("loan_id", col("id"))
            .withColumn("loan_amount", expr("10000 + rand() * 990000"))
            .withColumn("interest_rate", expr("0.02 + rand() * 0.18"))
            .withColumn("loan_term", expr("12 * (1 + cast(rand() * 29 as int))"))
            .withColumn("loan_type", expr("array('Personal', 'Mortgage', 'Auto', 'Business')[cast(rand() * 4 as int)]"))
            .withColumn("loan_status", expr("array('Active', 'Paid Off', 'Defaulted', 'Late')[cast(rand() * 4 as int)]"))
            .withColumn("origination_date", expr("date_sub(current_date(), cast(rand() * 365 * 5 as int))"))
            .withColumn("maturity_date", expr("date_add(origination_date, loan_term * 30)"))
            .withColumn("payment_frequency", expr("array('Monthly', 'Bi-weekly', 'Weekly')[cast(rand() * 3 as int)]"))
            .withColumn("monthly_payment", expr("loan_amount * (interest_rate / 12) / (1 - power(1 + (interest_rate / 12), -loan_term))"))
            .withColumn("total_payments_made", expr("cast(rand() * loan_term as int)"))
            .withColumn("remaining_balance", expr("loan_amount - (monthly_payment * total_payments_made)"))
            .withColumn("last_payment_date", expr("date_sub(current_date(), cast(rand() * 30 as int))"))
            .withColumn("next_payment_date", expr("date_add(last_payment_date, 30)"))
            .withColumn("days_past_due", expr("cast(rand() * 90 as int)"))
            .withColumn("times_30_days_late", expr("cast(rand() * 5 as int)"))
            .withColumn("times_60_days_late", expr("cast(rand() * 3 as int)"))
            .withColumn("times_90_days_late", expr("cast(rand() * 2 as int)"))
            .withColumn("collateral_type", expr("array('Real Estate', 'Vehicle', 'Securities', 'None')[cast(rand() * 4 as int)]"))
            .withColumn("collateral_value", expr("loan_amount * (0.8 + rand() * 0.4)"))
            .withColumn("loan_to_value_ratio", expr("loan_amount / collateral_value"))
            .withColumn("debt_to_income_ratio", expr("rand() * 0.5"))
            .withColumn("credit_score_at_origination", expr("300 + cast(rand() * 550 as int)"))
            .withColumn("current_credit_score", expr("300 + cast(rand() * 550 as int)"))
            .withColumn("employment_status", expr("array('Employed', 'Self-employed', 'Unemployed', 'Retired')[cast(rand() * 4 as int)]"))
            .withColumn("income_verification", expr("array('Verified', 'Stated')[cast(rand() * 2 as int)]"))
            .withColumn("loan_purpose", expr("array('Home purchase', 'Refinance', 'Home improvement', 'Debt consolidation', 'Business', 'Other')[cast(rand() * 6 as int)]"))
            .withColumn("branch_id", expr("cast(rand() * 1000 as int)"))
            .withColumn("underwriter_id", expr("cast(rand() * 500 as int)"))
            .select(*[column for column in loan_schema.fieldNames()])
            )

# COMMAND ----------
# Generate datasets
num_customers = 40_000_000
num_loans = 4_000_000

print("Generating customer data...")
customer_df = generate_customer_data(num_customers)

# Select one customer for the skewed distribution
skewed_customer = customer_df.limit(1).collect()[0]
skewed_customer_id = skewed_customer.customer_id

# Sample other customers for the remaining loans
other_customers = customer_df.filter(col("customer_id") != skewed_customer_id).limit(int(num_loans * 0.03))
other_customer_ids = [row.customer_id for row in other_customers.select("customer_id").collect()]

print("Generating loan data...")
loan_df = generate_loan_data(num_loans, skewed_customer_id, other_customer_ids)

# COMMAND ----------
# Write data to S3
s3_bucket = "your-s3-bucket-name"
customer_path = f"s3a://{s3_bucket}/customer_data.csv"
loan_path = f"s3a://{s3_bucket}/loan_data.csv"

print("Writing customer data to S3...")
customer_df.write.csv(customer_path, header=True, mode="overwrite")

print("Writing loan data to S3...")
loan_df.write.csv(loan_path, header=True, mode="overwrite")

print("Data generation and writing complete.")

# COMMAND ----------
# Verify data
print("Customer data sample:")
spark.read.csv(customer_path, header=True, schema=customer_schema).show(5)

print("Loan data sample:")
spark.read.csv(loan_path, header=True, schema=loan_schema).show(5)

# COMMAND ----------
# Verify skewed distribution
loan_distribution = loan_df.groupBy("customer_id").count().orderBy(col("count").desc())
print("Loan distribution:")
loan_distribution.show(10)

# Verify join scenario
joined_df = loan_df.join(customer_df, "customer_id", "left")
total_loans = joined_df.count()
matched_loans = joined_df.filter(col("first_name").isNotNull()).count()
unmatched_loans = joined_df.filter(col("first_name").isNull()).count()
skewed_customer_loans = joined_df.filter(col("customer_id") == skewed_customer_id).count()

print(f"Total loans: {total_loans}")
print(f"Loans with matching customers: {matched_loans}")
print(f"Loans without matching customers: {unmatched_loans}")
print(f"Percentage of loans with matching customers: {matched_loans / total_loans * 100:.2f}%")
print(f"Loans belonging to the skewed customer: {skewed_customer_loans}")
print(f"Percentage of loans belonging to the skewed customer: {skewed_customer_loans / total_loans * 100:.2f}%")

# COMMAND ----------
# Clean up
spark.catalog.clearCache()