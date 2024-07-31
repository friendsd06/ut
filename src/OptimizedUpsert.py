from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, rand, expr, broadcast
from delta.tables import DeltaTable
import uuid
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, DateType

# Initialize Spark Session with optimized configurations
spark = (SparkSession.builder
         .appName("OptimizedUpsertExample")
         .config("spark.sql.shuffle.partitions", "200")
         .config("spark.default.parallelism", "100")
         .config("spark.sql.broadcastTimeout", "600")
         .config("spark.sql.adaptive.enabled", "true")
         .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
         .config("spark.sql.adaptive.skewJoin.enabled", "true")
         .config("spark.databricks.delta.optimizeWrite.enabled", "true")
         .config("spark.databricks.delta.autoCompact.enabled", "true")
         .config("spark.databricks.delta.properties.defaults.checkpointRetentionDuration", "30 days")
         .getOrCreate())

# Define the schema for our table
schema = StructType([
    StructField("id", StringType(), False),
    StructField("name", StringType(), False),
    StructField("age", IntegerType(), False),
    StructField("salary", DoubleType(), False),
    StructField("department", StringType(), False),
    StructField("last_updated", DateType(), False)
])

# Create initial data (more than 1000 records)
data = [(str(uuid.uuid4()), f"Name_{i}", 20 + i % 40, 30000 + (i * 1000 % 70000), f"Dept_{i % 5}", expr("current_date()")) for i in range(1500)]

# Create DataFrame
df = spark.createDataFrame(data, schema)

# Write data to Delta table with optimized write
table_name = "employee_table"
(df.repartition(20)
 .write
 .format("delta")
 .mode("overwrite")
 .option("overwriteSchema", "true")
 .saveAsTable(table_name))

print(f"Created table '{table_name}' with {df.count()} records")

# Create source data for upsert (mix of updates and new records)
update_data = [(str(uuid.uuid4()), f"Updated_Name_{i}", 25 + i % 35, 35000 + (i * 1500 % 80000), f"Dept_{i % 6}", expr("current_date()")) for i in range(1000, 2000)]
source_df = spark.createDataFrame(update_data, schema)

# Optimize source data
source_df = (source_df
             .repartition(20, "id")  # Repartition for better distribution
             .sortWithinPartitions("id"))  # Sort within partitions for better merge performance

# Cache the source DataFrame
source_df.cache()
source_df.count()  # Materialize the cache

# Perform the merge operation
target_table = DeltaTable.forName(spark, table_name)

# Use a smaller broadcast join threshold for this operation
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", 52428800)  # 50MB

merge_builder = (target_table.alias("target")
                 .merge(source_df.alias("source"), "target.id = source.id")
                 .whenMatchedUpdateAll(condition="source.salary > target.salary")
                 .whenNotMatchedInsertAll())

# Execute merge with dynamic partition overwrite
(merge_builder.option("spark.databricks.delta.merge.optimizeWrite.enabled", "true")
 .option("spark.databricks.delta.merge.optimizeWrite.binSize", "2048")
 .execute())

# Optimize the table after merge
target_table.optimize().executeCompaction()

# Verify the results
result_df = spark.table(table_name)
print(f"\nFinal record count: {result_df.count()}")
print("\nSample data after upsert:")
result_df.show(10, truncate=False)

# Clean up
spark.catalog.clearCache()