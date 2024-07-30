from pyspark.sql import SparkSession
from dbldatagen import DataGenerator, fakergen
from pyspark.sql.functions import col, expr
import time

spark = SparkSession.builder.appName("CartesianProductExplosion").getOrCreate()

# Generate two datasets
users_gen = (DataGenerator(spark, name="users", rowcount=10000, partitions=10)
             .withColumn("user_id", expr("uuid()"))
             .withColumn("name", fakergen("name"))
             )

products_gen = (DataGenerator(spark, name="products", rowcount=1000, partitions=5)
                .withColumn("product_id", expr("uuid()"))
                .withColumn("product_name", fakergen("word"))
                )

users_df = users_gen.build()
products_df = products_gen.build()

# Perform a cartesian join (cross join)
start_time = time.time()
result_df = users_df.crossJoin(products_df)
result_count = result_df.count()
end_time = time.time()

print(f"Cartesian product completed. Result count: {result_count}")
print(f"Execution time: {end_time - start_time:.2f} seconds")
result_df.show(10)