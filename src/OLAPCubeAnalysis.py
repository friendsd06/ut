from pyspark.sql import SparkSession
from dbldatagen import DataGenerator, fakergen
from pyspark.sql.functions import col, expr, sum as spark_sum, count, avg, max as spark_max, min as spark_min
import time

spark = SparkSession.builder.appName("OLAPCubeAnalysis").getOrCreate()

# Generate a large sales dataset
data_gen = (DataGenerator(spark, name="sales_data", rowcount=100_000_000, partitions=100)
            .withColumn("transaction_id", expr("uuid()"))
            .withColumn("date", expr("date_sub(current_date(), int(rand() * 365 * 3))"))  # Last 3 years
            .withColumn("product_id", expr("concat('PROD_', cast(rand() * 1000 as int))"))
            .withColumn("category", expr("array('Electronics', 'Clothing', 'Food', 'Books', 'Home')[int(rand() * 5)]"))
            .withColumn("store_id", expr("concat('STORE_', cast(rand() * 100 as int))"))
            .withColumn("region", expr("array('North', 'South', 'East', 'West')[int(rand() * 4)]"))
            .withColumn("customer_id", expr("concat('CUST_', cast(rand() * 1000000 as int))"))
            .withColumn("quantity", expr("cast(rand() * 10 + 1 as int)"))
            .withColumn("unit_price", expr("rand() * 1000"))
            .withColumn("total_price", expr("quantity * unit_price"))
            )

df = data_gen.build()

# Extract date dimensions
df = df.withColumn("year", expr("year(date)"))
df = df.withColumn("quarter", expr("quarter(date)"))
df = df.withColumn("month", expr("month(date)"))

# Define dimensions and measures for the OLAP cube
dimensions = ["year", "quarter", "month", "category", "region", "store_id", "product_id"]
measures = [
    spark_sum("total_price").alias("total_sales"),
    count("transaction_id").alias("num_transactions"),
    avg("unit_price").alias("avg_unit_price"),
    spark_sum("quantity").alias("total_quantity"),
    spark_max("unit_price").alias("max_unit_price"),
    spark_min("unit_price").alias("min_unit_price")
]

# Generate OLAP cube
start_time = time.time()
cube = df.cube(*dimensions).agg(*measures)
cube.cache()
cube_count = cube.count()
end_time = time.time()

print(f"OLAP cube generated. Number of rows: {cube_count}")
print(f"Execution time: {end_time - start_time:.2f} seconds")

# Perform some analytical queries on the cube
print("\nTop 10 product categories by total sales:")
cube.filter(col("category").isNotNull()) \
    .groupBy("category") \
    .agg(spark_sum("total_sales").alias("category_total_sales")) \
    .orderBy(col("category_total_sales").desc()) \
    .show(10)

print("\nQuarterly sales trend:")
cube.filter((col("year").isNotNull()) & (col("quarter").isNotNull()) & col("month").isNull()) \
    .groupBy("year", "quarter") \
    .agg(spark_sum("total_sales").alias("quarterly_sales")) \
    .orderBy("year", "quarter") \
    .show()

print("\nTop 5 stores by number of transactions:")
cube.filter(col("store_id").isNotNull()) \
    .groupBy("store_id") \
    .agg(spark_sum("num_transactions").alias("store_transactions")) \
    .orderBy(col("store_transactions").desc()) \
    .show(5)

print("\nAverage unit price by region and category:")
cube.filter((col("region").isNotNull()) & (col("category").isNotNull())) \
    .groupBy("region", "category") \
    .agg(avg("avg_unit_price").alias("region_category_avg_price")) \
    .orderBy("region", col("region_category_avg_price").desc()) \
    .show()