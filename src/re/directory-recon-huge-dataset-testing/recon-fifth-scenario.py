from pyspark.sql import SparkSession
from pyspark.sql.functions import col, struct, when, rand, expr, lit, udf
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType
import random
from faker import Faker
from datetime import datetime, timedelta

# Initialize Spark session
spark = SparkSession.builder \
    .appName("MeaningfulDataGeneration") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.access.key", "YOUR_ACCESS_KEY") \
    .config("spark.hadoop.fs.s3a.secret.key", "YOUR_SECRET_KEY") \
    .getOrCreate()

# Initialize Faker
fake = Faker()

# Define schema
def create_schema():
    return StructType([
        # Customer Information (Struct)
        StructField("customer_info", StructType([
            StructField("customer_id", StringType(), True),
            StructField("name", StringType(), True),
            StructField("email", StringType(), True)
        ]), True),

        # Address Information (Struct)
        StructField("address_info", StructType([
            StructField("street", StringType(), True),
            StructField("city", StringType(), True),
            StructField("state", StringType(), True),
            StructField("zip_code", StringType(), True)
        ]), True),

        # Order Information (Struct)
        StructField("order_info", StructType([
            StructField("order_id", StringType(), True),
            StructField("order_date", TimestampType(), True),
            StructField("total_amount", DoubleType(), True)
        ]), True),

        # Product Information (Struct)
        StructField("product_info", StructType([
            StructField("product_id", StringType(), True),
            StructField("product_name", StringType(), True),
            StructField("category", StringType(), True)
        ]), True),

        # Payment Information (Struct)
        StructField("payment_info", StructType([
            StructField("payment_method", StringType(), True),
            StructField("card_type", StringType(), True),
            StructField("card_number", StringType(), True)
        ]), True),

        # Shipping Information (Struct)
        StructField("shipping_info", StructType([
            StructField("shipping_method", StringType(), True),
            StructField("tracking_number", StringType(), True),
            StructField("estimated_delivery", TimestampType(), True)
        ]), True),

        # Additional scalar fields
        *[StructField(f"string_field_{i}", StringType(), True) for i in range(1, 95)],
        *[StructField(f"int_field_{i}", IntegerType(), True) for i in range(1, 51)],
        *[StructField(f"double_field_{i}", DoubleType(), True) for i in range(1, 51)]
    ])

schema = create_schema()

# Generate meaningful data
@udf(schema)
def generate_row():
    customer_id = fake.uuid4()
    order_date = fake.date_time_between(start_date="-1y", end_date="now")
    return (
        # Customer Information
        (customer_id, fake.name(), fake.email()),

        # Address Information
        (fake.street_address(), fake.city(), fake.state(), fake.zipcode()),

        # Order Information
        (fake.uuid4(), order_date, round(random.uniform(10.0, 1000.0), 2)),

        # Product Information
        (fake.uuid4(), fake.product_name(), fake.category_name()),

        # Payment Information
        (fake.credit_card_provider(), fake.credit_card_provider(), fake.credit_card_number(masked=True)),

        # Shipping Information
        (random.choice(["Standard", "Express", "Overnight"]), fake.uuid4(), order_date + timedelta(days=random.randint(1, 7))),

        # Additional scalar fields
        *(fake.word() for _ in range(94)),
        *(random.randint(1, 1000) for _ in range(50)),
        *(round(random.uniform(1.0, 1000.0), 2) for _ in range(50))
    )

# Generate source and target DataFrames
def generate_dataframe(num_rows):
    return spark.range(num_rows).select(generate_row().alias("data")).select("data.*")

# Generate source DataFrame
num_rows = 10_000_000
source_df = generate_dataframe(num_rows)

# Generate target DataFrame with 90% similar data
target_df = generate_dataframe(int(num_rows * 0.9))
additional_rows = generate_dataframe(int(num_rows * 0.1))
target_df = target_df.union(additional_rows)

# Write DataFrames to S3
def write_to_s3(df, bucket, path):
    df.write.parquet(f"s3a://{bucket}/{path}", mode="overwrite")

# Write source DataFrame to S3
write_to_s3(source_df, "your-source-bucket", "source_data")

# Write target DataFrame to S3
write_to_s3(target_df, "your-target-bucket", "target_data")

print("Data generation and writing to S3 completed successfully.")