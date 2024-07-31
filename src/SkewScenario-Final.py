from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, rand, expr, current_timestamp, to_timestamp
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType
import uuid
from datetime import datetime

# Initialize Spark session
spark = SparkSession.builder.appName("LoanDataGeneration").getOrCreate()

# Define schemas
customer_schema = StructType([
    StructField("customer_id", StringType(), False),
    StructField("name", StringType(), False),
    StructField("type", StringType(), False),
    StructField("registration_date", TimestampType(), False)
])

loan_schema = StructType([
    StructField("loan_id", StringType(), False),
    StructField("customer_id", StringType(), False),
    StructField("loan_amount", DoubleType(), False),
    StructField("loan_date", TimestampType(), False),
    StructField("loan_type", StringType(), False)
])

# Generate customer data
def generate_customers():
    placeholder_date = datetime.now()  # Use a placeholder date

    individual_customers = [
        (str(uuid.uuid4()), f"Individual_{i}", "Individual", placeholder_date)
        for i in range(5_000_000)
    ]

    corporate_customers = [
        (str(uuid.uuid4()), f"Corporate_{i}", "Corporate", placeholder_date)
        for i in range(5)
    ]

    df = spark.createDataFrame(individual_customers + corporate_customers, customer_schema)

    return df.withColumn(
        "registration_date",
        when(col("type") == "Individual",
             expr("current_timestamp() - interval 1 year + interval cast(rand() * 365 as int) day"))
            .otherwise(expr("current_timestamp() - interval 5 year + interval cast(rand() * 1825 as int) day"))
    )

# Generate loan data
def generate_loans(customers_df):
    corporate_ids = [row.customer_id for row in customers_df.filter(col("type") == "Corporate").collect()]

    placeholder_date = datetime.now()  # Use a placeholder date

    def generate_loan_entry():
        if rand() < 0.98:  # 98% of loans to corporate customers
            return (
                str(uuid.uuid4()),
                corporate_ids[int(rand() * len(corporate_ids))],
                rand() * 10_000_000,
                placeholder_date,
                "Corporate"
            )
        else:
            return (
                str(uuid.uuid4()),
                str(uuid.uuid4()),  # Random UUID for individual customers
                rand() * 10_000,
                placeholder_date,
                "Personal"
            )

    df = spark.range(0, 100_000_000).rdd.map(lambda x: generate_loan_entry()).toDF(loan_schema)

    return df.withColumn(
        "loan_date",
        expr("current_timestamp() - interval cast(rand() * 1825 as int) day")
    )

# Generate and save customer data
print("Generating customer data...")
customers_df = generate_customers()
customers_df.write.csv("s3://your-bucket/customers", header=True, mode="overwrite")
print("Customer data saved to S3")

# Generate and save loan data
print("Generating loan data...")
loans_df = generate_loans(customers_df)
loans_df.write.csv("s3://your-bucket/loans", header=True, mode="overwrite")
print("Loan data saved to S3")

# Show some statistics
print("\nCustomer Distribution:")
customers_df.groupBy("type").count().show()

print("\nLoan Distribution:")
loans_df.groupBy("loan_type").agg(
    count("*").alias("loan_count"),
    expr("sum(loan_amount)").alias("total_loan_amount")
).show()

# Clean up
spark.stop()