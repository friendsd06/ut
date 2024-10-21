from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, array, struct, col, when, rand
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType, ArrayType, BooleanType
import random
from faker import Faker
from datetime import datetime, timedelta

# Initialize Spark session
spark = SparkSession.builder \
    .appName("EcommerceOrderDataGeneration") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.access.key", "YOUR_ACCESS_KEY") \
    .config("spark.hadoop.fs.s3a.secret.key", "YOUR_SECRET_KEY") \
    .getOrCreate()

# Initialize Faker
fake = Faker()

# Define schema
def create_schema():
    return StructType([
        # Basic Order Information
        StructField("order_id", StringType(), False),
        StructField("customer_id", StringType(), False),
        StructField("order_date", TimestampType(), False),
        StructField("order_status", StringType(), False),
        StructField("total_amount", DoubleType(), False),

        # Customer Information
        StructField("customer_name", StringType(), True),
        StructField("customer_email", StringType(), True),
        StructField("customer_phone", StringType(), True),

        # Billing Address
        StructField("billing_address_line1", StringType(), True),
        StructField("billing_address_line2", StringType(), True),
        StructField("billing_city", StringType(), True),
        StructField("billing_state", StringType(), True),
        StructField("billing_zip", StringType(), True),
        StructField("billing_country", StringType(), True),

        # Shipping Address
        StructField("shipping_address_line1", StringType(), True),
        StructField("shipping_address_line2", StringType(), True),
        StructField("shipping_city", StringType(), True),
        StructField("shipping_state", StringType(), True),
        StructField("shipping_zip", StringType(), True),
        StructField("shipping_country", StringType(), True),

        # Payment Information
        StructField("payment_method", StringType(), True),
        StructField("card_type", StringType(), True),
        StructField("card_last_four", StringType(), True),

        # Shipping Information
        StructField("shipping_method", StringType(), True),
        StructField("shipping_cost", DoubleType(), True),
        StructField("estimated_delivery_date", TimestampType(), True),

        # Nested Array of Struct: Order Items
        StructField("order_items", ArrayType(StructType([
            StructField("item_id", StringType(), True),
            StructField("product_id", StringType(), True),
            StructField("product_name", StringType(), True),
            StructField("quantity", IntegerType(), True),
            StructField("unit_price", DoubleType(), True),
            StructField("subtotal", DoubleType(), True)
        ])), True),

        # Nested Array of Struct: Shipping Updates
        StructField("shipping_updates", ArrayType(StructType([
            StructField("update_id", StringType(), True),
            StructField("update_date", TimestampType(), True),
            StructField("status", StringType(), True),
            StructField("location", StringType(), True),
            StructField("description", StringType(), True)
        ])), True),

        # Nested Array of Struct: Payment Transactions
        StructField("payment_transactions", ArrayType(StructType([
            StructField("transaction_id", StringType(), True),
            StructField("transaction_date", TimestampType(), True),
            StructField("amount", DoubleType(), True),
            StructField("status", StringType(), True),
            StructField("payment_gateway", StringType(), True)
        ])), True),

        # Nested Array of Struct: Product Reviews
        StructField("product_reviews", ArrayType(StructType([
            StructField("review_id", StringType(), True),
            StructField("product_id", StringType(), True),
            StructField("rating", IntegerType(), True),
            StructField("review_text", StringType(), True),
            StructField("review_date", TimestampType(), True)
        ])), True),

        # Nested Array of Struct: Order Tags
        StructField("order_tags", ArrayType(StructType([
            StructField("tag_id", StringType(), True),
            StructField("tag_name", StringType(), True),
            StructField("tag_value", StringType(), True)
        ])), True),

        # Additional scalar fields
        StructField("is_gift", BooleanType(), True),
        StructField("gift_message", StringType(), True),
        StructField("coupon_code", StringType(), True),
        StructField("discount_amount", DoubleType(), True),
        StructField("tax_amount", DoubleType(), True),
        StructField("customer_notes", StringType(), True),
        StructField("internal_notes", StringType(), True),
        StructField("is_expedited", BooleanType(), True),
        StructField("channel", StringType(), True),
        StructField("device_type", StringType(), True),
        StructField("browser", StringType(), True),
        StructField("ip_address", StringType(), True),
        StructField("session_id", StringType(), True),
        StructField("user_agent", StringType(), True),
        StructField("referral_source", StringType(), True),
        StructField("affiliate_id", StringType(), True),
        StructField("currency_code", StringType(), True),
        StructField("language_code", StringType(), True),
        StructField("customer_segment", StringType(), True)
    ])

schema = create_schema()

# Generate meaningful data
@udf(schema)
def generate_row():
    order_date = fake.date_time_between(start_date="-1y", end_date="now")
    total_amount = round(random.uniform(10.0, 1000.0), 2)

    # Generate order items
    num_items = random.randint(1, 5)
    order_items = []
    for _ in range(num_items):
        quantity = random.randint(1, 5)
        unit_price = round(random.uniform(5.0, 100.0), 2)
        order_items.append({
            "item_id": fake.uuid4(),
            "product_id": fake.uuid4(),
            "product_name": fake.product_name(),
            "quantity": quantity,
            "unit_price": unit_price,
            "subtotal": round(quantity * unit_price, 2)
        })

    # Generate shipping updates
    num_updates = random.randint(1, 3)
    shipping_updates = []
    for i in range(num_updates):
        update_date = order_date + timedelta(days=i+1)
        shipping_updates.append({
            "update_id": fake.uuid4(),
            "update_date": update_date,
            "status": random.choice(["Processing", "Shipped", "In Transit", "Delivered"]),
            "location": fake.city(),
            "description": fake.sentence()
        })

    # Generate payment transactions
    num_transactions = random.randint(1, 2)
    payment_transactions = []
    for i in range(num_transactions):
        transaction_date = order_date + timedelta(minutes=i*5)
        payment_transactions.append({
            "transaction_id": fake.uuid4(),
            "transaction_date": transaction_date,
            "amount": round(total_amount / num_transactions, 2),
            "status": random.choice(["Pending", "Completed", "Failed"]),
            "payment_gateway": random.choice(["PayPal", "Stripe", "Square"])
        })

    # Generate product reviews
    num_reviews = random.randint(0, num_items)
    product_reviews = []
    for _ in range(num_reviews):
        review_date = order_date + timedelta(days=random.randint(7, 30))
        product_reviews.append({
            "review_id": fake.uuid4(),
            "product_id": random.choice(order_items)["product_id"],
            "rating": random.randint(1, 5),
            "review_text": fake.paragraph(),
            "review_date": review_date
        })

    # Generate order tags
    num_tags = random.randint(0, 3)
    order_tags = []
    for _ in range(num_tags):
        order_tags.append({
            "tag_id": fake.uuid4(),
            "tag_name": random.choice(["VIP", "Loyalty", "First-time", "High-value"]),
            "tag_value": fake.word()
        })

    return (
        fake.uuid4(),  # order_id
        fake.uuid4(),  # customer_id
        order_date,
        random.choice(["Pending", "Processing", "Shipped", "Delivered"]),
        total_amount,
        fake.name(),
        fake.email(),
        fake.phone_number(),
        fake.street_address(),
        fake.secondary_address(),
        fake.city(),
        fake.state(),
        fake.zipcode(),
        fake.country(),
        fake.street_address(),
        fake.secondary_address(),
        fake.city(),
        fake.state(),
        fake.zipcode(),
        fake.country(),
        random.choice(["Credit Card", "PayPal", "Apple Pay", "Google Pay"]),
        random.choice(["Visa", "MasterCard", "Amex", "Discover"]),
        fake.credit_card_number(card_type=None)[-4:],
        random.choice(["Standard", "Express", "Overnight"]),
        round(random.uniform(5.0, 25.0), 2),
        order_date + timedelta(days=random.randint(1, 7)),
        order_items,
        shipping_updates,
        payment_transactions,
        product_reviews,
        order_tags,
        random.choice([True, False]),
        fake.text() if random.choice([True, False]) else None,
        fake.word().upper() + str(random.randint(10, 99)) if random.choice([True, False]) else None,
        round(random.uniform(0, 50.0), 2),
        round(total_amount * 0.1, 2),  # Assume 10% tax
        fake.text() if random.choice([True, False]) else None,
        fake.text() if random.choice([True, False]) else None,
        random.choice([True, False]),
        random.choice(["Web", "Mobile App", "Phone", "In-store"]),
        random.choice(["Desktop", "Mobile", "Tablet"]),
        random.choice(["Chrome", "Firefox", "Safari", "Edge"]),
        fake.ipv4(),
        fake.uuid4(),
        fake.user_agent(),
        random.choice(["Google", "Facebook", "Email", "Direct"]),
        fake.uuid4() if random.choice([True, False]) else None,
        random.choice(["USD", "EUR", "GBP", "CAD", "AUD"]),
        random.choice(["en", "es", "fr", "de", "it"]),
        random.choice(["New", "Returning", "VIP", "At-risk"])
    )

# Generate source and target DataFrames
def generate_dataframe(num_rows):
    return spark.range(num_rows).select(generate_row().alias("data")).select("data.*")

# Generate source DataFrame
num_rows = 1_000_000  # Adjusted to 1 million for faster processing
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
write_to_s3(source_df, "your-source-bucket", "ecommerce_orders_source")

# Write target DataFrame to S3
write_to_s3(target_df, "your-target-bucket", "ecommerce_orders_target")

print("E-commerce order data generation and writing to S3 completed successfully.")