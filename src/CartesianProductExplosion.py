from pyspark.sql import SparkSession
from dbldatagen import DataGenerator
from pyspark.sql.functions import expr
import time

spark = SparkSession.builder.appName("CartesianProductExplosion").getOrCreate()

# Generate two datasets
users_gen = (DataGenerator(spark, name="users", rows=10000, partitions=10)
             .withIdOutput()
             .withColumn("user_id", "string", expr("uuid()"))
             .withColumn("name", "string", expr="concat('User_', cast(id as string))")
             )

products_gen = (DataGenerator(spark, name="products", rows=1000, partitions=5)
                .withIdOutput()
                .withColumn("product_id", "string", expr="uuid()")
                .withColumn("product_name", "string", expr="concat('Product_', cast(id as string))")
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