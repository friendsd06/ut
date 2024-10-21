from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, array, struct, col, when, rand
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType, ArrayType, BooleanType
import random
from faker import Faker
from datetime import datetime, timedelta

# Initialize Spark session
spark = SparkSession.builder \
    .appName("EcommerceOrderDataGenerationArrayOfArray") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.access.key", "YOUR_ACCESS_KEY") \
    .config("spark.hadoop.fs.s3a.secret.key", "YOUR_SECRET_KEY") \
    .getOrCreate()

# Initialize Faker
fake = Faker()

# Define schema
def create_schema():
    return StructType([
        # ... (previous fields remain the same)

        # Modify these fields to be array of array
        StructField("order_item_price_history", ArrayType(ArrayType(DoubleType())), True),
        StructField("product_category_hierarchy", ArrayType(ArrayType(StringType())), True),
        StructField("customer_interaction_timeline", ArrayType(ArrayType(StringType())), True),

        # ... (rest of the fields remain the same)
    ])

schema = create_schema()

# Generate meaningful data
@udf(schema)
def generate_row():
    # ... (previous field generations remain the same)

    # Generate order item price history (array of array of doubles)
    num_items = random.randint(1, 5)
    order_item_price_history = []
    for _ in range(num_items):
        num_price_changes = random.randint(1, 5)
        price_history = [round(random.uniform(10.0, 100.0), 2) for _ in range(num_price_changes)]
        order_item_price_history.append(price_history)

    # Generate product category hierarchy (array of array of strings)
    num_products = random.randint(1, 5)
    product_category_hierarchy = []
    for _ in range(num_products):
        category_depth = random.randint(1, 4)
        category_path = [fake.category_name() for _ in range(category_depth)]
        product_category_hierarchy.append(category_path)

    # Generate customer interaction timeline (array of array of strings)
    num_interactions = random.randint(1, 10)
    customer_interaction_timeline = []
    for _ in range(num_interactions):
        interaction_details = [
            fake.date_time_this_year().isoformat(),
            random.choice(["Website Visit", "Product View", "Add to Cart", "Purchase", "Customer Support"]),
            fake.sentence()
        ]
        customer_interaction_timeline.append(interaction_details)

    return (
        # ... (previous return values remain the same)
        order_item_price_history,
        product_category_hierarchy,
        customer_interaction_timeline,
        # ... (rest of the return values remain the same)
    )

# Generate source and target DataFrames
def generate_dataframe(num_rows):
    return spark.range(num_rows).select(generate_row().alias("data")).select("data.*")

# Generate source DataFrame
num_rows = 1_000_000  # 1 million rows
source_df = generate_dataframe(num_rows)

# Generate target DataFrame with 95% similar data and 5% differences
num_similar_rows = int(num_rows * 0.95)
num_different_rows = num_rows - num_similar_rows

target_df_similar = generate_dataframe(num_similar_rows)
target_df_different = generate_dataframe(num_different_rows)

# Introduce small differences in the similar part of target DataFrame
columns_to_modify = ["total_amount", "shipping_cost", "estimated_delivery_date", "customer_segment"]
for column in columns_to_modify:
    if column in ["total_amount", "shipping_cost"]:
        target_df_similar = target_df_similar.withColumn(
            column,
            when(rand() < 0.05, col(column) * (1 + (rand() - 0.5) * 0.1)).otherwise(col(column))
        )
    elif column == "estimated_delivery_date":
        target_df_similar = target_df_similar.withColumn(
            column,
            when(rand() < 0.05, col(column) + (rand() * 2 - 1) * 86400).otherwise(col(column))
        )
    else:
        target_df_similar = target_df_similar.withColumn(
            column,
            when(rand() < 0.05, fake.random_element(elements=("New", "Returning", "VIP", "At-risk"))).otherwise(col(column))
        )

# Combine similar and different parts of target DataFrame
target_df = target_df_similar.union(target_df_different)

# Write DataFrames to S3
def write_to_s3(df, bucket, path):
    df.write.parquet(f"s3a://{bucket}/{path}", mode="overwrite")

# Write source DataFrame to S3
write_to_s3(source_df, "your-source-bucket", "ecommerce_orders_source_array_of_array")

# Write target DataFrame to S3
write_to_s3(target_df, "your-target-bucket", "ecommerce_orders_target_array_of_array")

print("E-commerce order data generation with array of arrays and writing to S3 completed successfully.")