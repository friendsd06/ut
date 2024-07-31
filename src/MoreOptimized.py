from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, rand, expr, broadcast, sha1
from delta.tables import DeltaTable
import uuid
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, DateType

# Initialize Spark Session with highly optimized configurations
spark = (SparkSession.builder
         .appName("HighlyOptimizedUpsertExample")
         .config("spark.sql.shuffle.partitions", "400")
         .config("spark.default.parallelism", "200")
         .config("spark.sql.broadcastTimeout", "1200")
         .config("spark.sql.adaptive.enabled", "true")
         .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
         .config("spark.sql.adaptive.skewJoin.enabled", "true")
         .config("spark.sql.adaptive.localShuffleReader.enabled", "true")
         .config("spark.sql.adaptive.skewJoin.skewedPartitionFactor", "10")
         .config("spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes", "256MB")
         .config("spark.sql.adaptive.nonEmptyPartitionRatioForBroadcastJoin", "0.2")
         .config("spark.databricks.delta.optimizeWrite.enabled", "true")
         .config("spark.databricks.delta.autoCompact.enabled", "true")
         .config("spark.databricks.delta.properties.defaults.checkpointRetentionDuration", "30 days")
         .config("spark.databricks.delta.merge.optimizeWrite.enabled", "true")
         .config("spark.databricks.delta.stalenessLimit", "7 days")
         .config("spark.databricks.delta.merge.repartitionBeforeWrite.enabled", "true")
         .config("spark.databricks.delta.schema.autoMerge.enabled", "true")
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

# Write data to Delta table with optimized write and Z-ordering
table_name = "employee_table"
(df.repartition(20, "id")  # Repartition by id for better data distribution
 .sortWithinPartitions("id")  # Sort within partitions
 .write
 .format("delta")
 .mode("overwrite")
 .option("overwriteSchema", "true")
 .option("dataChange", "false")  # Indicate this is not a data change operation
 .saveAsTable(table_name))

print(f"Created table '{table_name}' with {df.count()} records")

# Z-order the table by id and department
spark.sql(f"OPTIMIZE {table_name} ZORDER BY (id, department)")

# Create source data for upsert (mix of updates and new records)
update_data = [(str(uuid.uuid4()), f"Updated_Name_{i}", 25 + i % 35, 35000 + (i * 1500 % 80000), f"Dept_{i % 6}", expr("current_date()")) for i in range(1000, 2000)]
source_df = spark.createDataFrame(update_data, schema)

# Optimize source data with salting to handle skew
salt_factor = 10
source_df = (source_df
             .withColumn("salt", (sha1(col("id")) % salt_factor).cast("int"))
             .repartition(20 * salt_factor, "salt", "id")
             .sortWithinPartitions("salt", "id")
             .drop("salt"))

# Cache the source DataFrame
source_df.cache()
source_df.count()  # Materialize the cache

# Perform the merge operation
target_table = DeltaTable.forName(spark, table_name)

# Use a dynamic broadcast join threshold
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")  # Disable static threshold
spark.conf.set("spark.sql.adaptive.autoBroadcastJoinThreshold", "100MB")  # Set dynamic threshold

# Merge with optimized write and dynamic partition overwrite
merge_builder = (target_table.alias("target")
                 .merge(source_df.alias("source"), "target.id = source.id")
                 .whenMatchedUpdateAll(condition="source.salary > target.salary")
                 .whenNotMatchedInsertAll())

(merge_builder
 .option("spark.databricks.delta.merge.optimizeWrite.enabled", "true")
 .option("spark.databricks.delta.merge.optimizeWrite.binSize", "4096")
 .option("spark.databricks.delta.merge.repartitionBeforeWrite.enabled", "true")
 .execute())

# Optimize the table after merge
target_table.optimize().executeCompaction()

# Vacuum old files
target_table.vacuum(168)  # 168 hours = 7 days

# Verify the results
result_df = spark.table(table_name)
print(f"\nFinal record count: {result_df.count()}")
print("\nSample data after upsert:")
result_df.show(10, truncate=False)

# Analyze table for better statistics
spark.sql(f"ANALYZE TABLE {table_name} COMPUTE STATISTICS FOR ALL COLUMNS")

# Clean up
spark.catalog.clearCache()


Key additional optimizations in this version:

Enhanced Spark Configurations: Added more fine-tuned configurations for adaptive query execution, skew handling, and Delta Lake optimizations.
Z-Ordering: Applied Z-ordering on the initial table write for better data locality.
Salting for Skew Handling: Implemented a salting technique on the source data to mitigate potential skew during the merge operation.
Optimized Initial Write: Added sorting within partitions during the initial table write for better performance.
    Dynamic Broadcast Join Threshold: Used a dynamic broadcast join threshold instead of a static one for more adaptive join strategies.
Merge Optimizations: Enabled additional Delta merge optimizations including repartitioning before write.
Post-merge Operations: Added table vacuuming to clean up old files and analyze table statistics for better query planning.
Schema Auto-merge: Enabled schema auto-merge for handling potential schema evolution.

These advanced optimizations should provide significant performance improvements, especially for large-scale operations and skewed data distributions. The salting technique, in particular, can be very effective in handling skew during merges.
Remember that some of these optimizations (like the specific configuration values) might need tuning based on your specific data characteristics and cluster resources. Always test with representative data volumes and monitor performance to find the optimal settings for your use case.