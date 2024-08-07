from pyspark.sql import SparkSession
from dbldatagen import DataGenerator
from pyspark.sql.functions import col, expr
import time

spark = SparkSession.builder.appName("JoinWithDataSkew").getOrCreate()

spark.conf.set("spark.executor.memory", "2g")
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")  # Disable broadcast joins

# Generate two large datasets with skewed join keys
left_gen = (DataGenerator(spark, name="left_data", rows=50_000_000, partitions=100)
            .withIdOutput()
            .withColumn("join_key", "string", expr="""
        case
            when rand() < 0.05 then 'SKEWED_KEY'
            else concat('KEY_', cast(rand() * 1000000 as int))
        end
    """)
            .withColumn("value", "double", expr="rand() * 1000")
            )

right_gen = (DataGenerator(spark, name="right_data", rows=10_000_000, partitions=50)
             .withIdOutput()
             .withColumn("join_key", "string", expr="""
        case
            when rand() < 0.1 then 'SKEWED_KEY'
            else concat('KEY_', cast(rand() * 1000000 as int))
        end
    """)
             .withColumn("info", "string", expr="concat('Info_', cast(rand() * 1000000 as int))")
             )

left_df = left_gen.build()
right_df = right_gen.build()

# Perform a join operation
start_time = time.time()
result_df = left_df.join(right_df, "join_key")
result_count = result_df.count()
end_time = time.time()

print(f"Join operation completed. Result count: {result_count}")
print(f"Execution time: {end_time - start_time:.2f} seconds")
result_df.groupBy("join_key").count().orderBy(col("count").desc()).show(10)