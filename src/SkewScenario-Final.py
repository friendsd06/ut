
from pyspark.sql.functions import udf, rand, when, lit, array, element_at
from pyspark.sql.types import StringType, IntegerType
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
    # Get corporate customer IDs
    corporate_ids = [row.customer_id for row in customers_df.filter(col("type") == "Corporate").collect()]

    # UDF to generate UUID
    uuidUdf = udf(lambda: str(uuid.uuid4()), StringType())

    # Create an array of corporate IDs
    corporate_id_array = array([lit(id) for id in corporate_ids])

    # Generate base dataframe
    base_df = spark.range(0, 100_000_000)

    # Generate loans
    loans_df = base_df.withColumn("random", rand()) \
        .withColumn("loan_id", uuidUdf()) \
        .withColumn("customer_id",
                    when(col("random") < 0.98,
                         element_at(corporate_id_array, (rand() * lit(len(corporate_ids))).cast(IntegerType()) + 1))
                    .otherwise(uuidUdf())) \
        .withColumn("loan_amount",
                    when(col("random") < 0.98, (rand() * 10_000_000).cast(IntegerType()))
                    .otherwise((rand() * 10_000).cast(IntegerType()))) \
        .withColumn("loan_type",
                    when(col("random") < 0.98, lit("Corporate"))
                    .otherwise(lit("Personal"))) \
        .select("loan_id", "customer_id", "loan_amount", "loan_type")

    return loans_df

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