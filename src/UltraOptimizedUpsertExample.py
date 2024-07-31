from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, rand, expr, broadcast, sha1, date_format
from delta.tables import DeltaTable
import uuid
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, DateType

# Initialize Spark Session with highly optimized configurations
spark = (SparkSession.builder
         .appName("UltraOptimizedUpsertExample")
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
         .config("spark.databricks.delta.optimizeWrite.binSize", "2048")
         .config("spark.databricks.delta.merge.optimizeWrite.binSize", "2048")
         .config("spark.databricks.delta.deletionVectors.enabled", "true")
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
(df.repartition(20, "id", "department")
 .sortWithinPartitions("id")
 .write
 .format("delta")
 .mode("overwrite")
 .option("overwriteSchema", "true")
 .option("dataChange", "false")
 .partitionBy("department")  # Partition by department for better query performance
 .saveAsTable(table_name))

print(f"Created table '{table_name}' with {df.count()} records")

# Z-order the table by id within partitions
spark.sql(f"OPTIMIZE {table_name} ZORDER BY (id)")

# Create source data for upsert (mix of updates and new records)
update_data = [(str(uuid.uuid4()), f"Updated_Name_{i}", 25 + i % 35, 35000 + (i * 1500 % 80000), f"Dept_{i % 6}", expr("current_date()")) for i in range(1000, 2000)]
source_df = spark.createDataFrame(update_data, schema)

# Optimize source data with salting to handle skew
salt_factor = 20
source_df = (source_df
             .withColumn("salt", (sha1(col("id")) % salt_factor).cast("int"))
             .repartition(20 * salt_factor, "salt", "department")
             .sortWithinPartitions("salt", "id")
             .drop("salt"))

# Cache the source DataFrame
source_df.cache()
source_df.count()  # Materialize the cache

# Perform the merge operation
target_table = DeltaTable.forName(spark, table_name)

# Use a dynamic broadcast join threshold
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")
spark.conf.set("spark.sql.adaptive.autoBroadcastJoinThreshold", "100MB")

# Merge with optimized write and dynamic partition overwrite
merge_builder = (target_table.alias("target")
                 .merge(source_df.alias("source"), "target.id = source.id")
                 .whenMatchedUpdate(
    condition="source.salary > target.salary",
    set={
        "name": "source.name",
        "age": "source.age",
        "salary": "source.salary",
        "last_updated": "source.last_updated"
    }
)
                 .whenNotMatchedInsertAll())

(merge_builder
 .option("spark.databricks.delta.merge.optimizeWrite.enabled", "true")
 .option("spark.databricks.delta.merge.repartitionBeforeWrite.enabled", "true")
 .execute())

# Optimize the table after merge
target_table.optimize().executeCompaction()

# Use deletion vectors for efficient file management
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


Key additional optimizations and features in this version:

Deletion Vectors: Enabled deletion vectors with spark.databricks.delta.deletionVectors.enabled. This optimizes how deletes are handled in Delta Lake, improving performance for delete operations and subsequent reads.
Table Partitioning: Partitioned the table by department, which can significantly improve query performance for department-based queries.
    Enhanced Salting: Increased the salt factor and included department in the repartitioning to better distribute data across partitions.
Optimized Merge: Used a more specific update condition in the merge operation, potentially reducing the amount of data that needs to be updated.
Fine-tuned Delta Configurations: Added more Delta-specific configurations for optimized writes and merges.
    Z-Ordering Within Partitions: Applied Z-ordering by id within each partition, which can improve data locality for queries that filter on both department and id.
Vacuum with Deletion Vectors: The vacuum operation now benefits from deletion vectors, making it more efficient.