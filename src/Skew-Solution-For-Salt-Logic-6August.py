from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, rand, lit, count, date_add, current_date
import matplotlib.pyplot as plt

# Initialize Spark session
spark = SparkSession.builder.appName("HighlySkewedTransactionExample").getOrCreate()

# Generate highly skewed transaction data
def generate_transactions(num_transactions):
    return (spark.range(num_transactions)
            .withColumn("transaction_id", col("id").cast("string"))
            .withColumn("customer_id",
                        when(rand() < 0.90, lit("customer_1"))
                        .otherwise((rand() * 999 + 2).cast("int").cast("string")))
            .withColumn("amount", (rand() * 1000).cast("decimal(10,2)"))
            .withColumn("product_id", (rand() * 100).cast("int").cast("string"))
            .withColumn("transaction_date", date_add(current_date(), -(rand() * 365).cast("int")))
            .withColumn("store_id", (rand() * 50).cast("int").cast("string")))

# Create transaction data
num_transactions = 1000000
transactions = generate_transactions(num_transactions)

# Show sample data
print("Sample of transaction data:")
transactions.show(5)

# Analyze distribution
distribution = transactions.groupBy("customer_id").count().orderBy("count", ascending=False)
print("Customer transaction distribution:")
distribution.show()

# Calculate percentage for top customer
top_customer_count = distribution.first()['count']
top_customer_percentage = (top_customer_count / num_transactions) * 100
print(f"Top customer percentage: {top_customer_percentage:.2f}%")

# Visualize distribution
plt.figure(figsize=(12, 6))
plt.bar(distribution.toPandas()["customer_id"][:20], distribution.toPandas()["count"][:20])
plt.title("Customer Transaction Distribution (Top 20)")
plt.xlabel("Customer ID")
plt.ylabel("Number of Transactions")
plt.xticks(rotation=45)
plt.tight_layout()
plt.show()

# Create customer data
customer_data = spark.createDataFrame([
                                          ("customer_1", "John Doe", "Premium"),
                                          ("customer_2", "Jane Smith", "Regular"),
                                          ("customer_3", "Alice Johnson", "Regular")
                                      ] + [("customer_" + str(i), "Customer " + str(i), "Regular") for i in range(4, 1001)],
                                      ["customer_id", "name", "category"])

# Add salting logic here
from pyspark.sql.functions import concat  # Add this import

# Function to salt a DataFrame
def salt_dataframe(df, key_column, num_salts):
    return df.withColumn("salt", (rand() * num_salts).cast("int")) \
        .withColumn("salted_key", concat(col(key_column), lit("_"), col("salt").cast("string")))

# Apply salting to transactions and customer_data
num_salts = 10
salted_transactions = salt_dataframe(transactions, "customer_id", num_salts)
salted_customer_data = salt_dataframe(customer_data, "customer_id", num_salts)

# Perform salted join
print("\nPerforming salted join...")
salted_join = salted_transactions.join(salted_customer_data, "salted_key")

# Remove salt column and deduplicate if necessary
final_join = salted_join.drop("salt", "salted_key").dropDuplicates(["transaction_id", "customer_id"])

print("Sample of joined data:")
final_join.show(5)

# Analyze join result
print("Join result distribution:")
final_join.groupBy("category").count().orderBy("count", ascending=False).show()