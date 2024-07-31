from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, rand, expr
import uuid

# Initialize Spark session
spark = SparkSession.builder.appName("SimpleSkewedLoanData").getOrCreate()

# Generate customer data
def generate_customers():
    individual_customers = [
        (str(uuid.uuid4()), f"Individual_{i}", "Individual")
        for i in range(5_000_000)
    ]

    corporate_customers = [
        (str(uuid.uuid4()), f"Corporate_{i}", "Corporate")
        for i in range(5)
    ]

    return spark.createDataFrame(individual_customers + corporate_customers,
                                 ["customer_id", "name", "type"])

# Generate loan data
def generate_loans(customers_df):
    corporate_ids = [row.customer_id for row in customers_df.filter(col("type") == "Corporate").collect()]

    def generate_loan_entry():
        if rand() < 0.98:  # 98% of loans to corporate customers
            return (
                str(uuid.uuid4()),
                corporate_ids[int(rand() * len(corporate_ids))],
                int(rand() * 10_000_000),
                "Corporate"
            )
        else:
            return (
                str(uuid.uuid4()),
                str(uuid.uuid4()),  # Random UUID for individual customers
                int(rand() * 10_000),
                "Personal"
            )

    return spark.range(0, 100_000_000).rdd.map(lambda x: generate_loan_entry()).toDF(
        ["loan_id", "customer_id", "loan_amount", "loan_type"])

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