from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, rand, expr, sum as sum_, count
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, TimestampType
import uuid
import time


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
    individual_customers = [
        (str(uuid.uuid4()), f"Individual_{i}", "Individual", expr("current_timestamp() - interval 1 year + interval cast(rand() * 365 as int) day"))
        for i in range(5_000_000)
    ]
    corporate_customers = [
        (str(uuid.uuid4()), f"Corporate_{i}", "Corporate", expr("current_timestamp() - interval 5 year + interval cast(rand() * 1825 as int) day"))
        for i in range(5)
    ]
    return spark.createDataFrame(individual_customers + corporate_customers, customer_schema)

# Generate loan data
def generate_loans(customers_df):
    corporate_ids = [row.customer_id for row in customers_df.filter(col("type") == "Corporate").collect()]

    def generate_loan_entry():
        if rand() < 0.98:  # 98% of loans to corporate customers
            return (
                str(uuid.uuid4()),
                corporate_ids[int(rand() * len(corporate_ids))],
                rand() * 10_000_000,
                expr("current_timestamp() - interval cast(rand() * 1825 as int) day"),
                "Corporate"
            )
        else:
            return (
                str(uuid.uuid4()),
                expr("uuid()"),
                rand() * 10_000,
                expr("current_timestamp() - interval cast(rand() * 365 as int) day"),
                "Personal"
            )

    return spark.range(0, 100_000_000).rdd.map(lambda x: generate_loan_entry()).toDF(loan_schema)

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