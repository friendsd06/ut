from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, rand, expr, broadcast, sha1, date_format, year, month, dayofmonth
from delta.tables import DeltaTable
import uuid
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, DateType

# Initialize Spark Session with highly optimized configurations
spark = (SparkSession.builder
         .appName("UltraOptimizedUpsertExample")
         .config("spark.sql.shuffle.partitions", "800")
         .config("spark.default.parallelism", "400")
         .config("spark.sql.broadcastTimeout", "1800")
         .config("spark.sql.adaptive.enabled", "true")
         .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
         .config("spark.sql.adaptive.skewJoin.enabled", "true")
         .config("spark.sql.adaptive.localShuffleReader.enabled", "true")
         .config("spark.sql.adaptive.skewJoin.skewedPartitionFactor", "5")
         .config("spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes", "512MB")
         .config("spark.sql.adaptive.nonEmptyPartitionRatioForBroadcastJoin", "0.3")
         .config("spark.databricks.delta.optimizeWrite.enabled", "true")
         .config("spark.databricks.delta.autoCompact.enabled", "true")
         .config("spark.databricks.delta.properties.defaults.checkpointRetentionDuration", "30 days")
         .config("spark.databricks.delta.merge.optimizeWrite.enabled", "true")
         .config("spark.databricks.delta.stalenessLimit", "7 days")
         .config("spark.databricks.delta.merge.repartitionBeforeWrite.enabled", "true")
         .config("spark.databricks.delta.schema.autoMerge.enabled", "true")
         .config("spark.databricks.delta.optimizeWrite.binSize", "1024")
         .config("spark.databricks.delta.merge.optimizeWrite.binSize", "1024")
         .config("spark.databricks.delta.deletionVectors.enabled", "true")
         .config("spark.databricks.delta.optimize.maxFileSize", "512mb")
         .config("spark.databricks.delta.properties.defaults.enableChangeDataFeed", "true")
         .config("spark.databricks.delta.merge.maxInsertCountForRangeScan", "100000")
         .config("spark.databricks.delta.merge.optimizeInsertOnlyMerge.enabled", "true")
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
(df.repartition(40, "id", "department")
 .sortWithinPartitions("id")
 .write
 .format("delta")
 .mode("overwrite")
 .option("overwriteSchema", "true")
 .option("dataChange", "false")
 .partitionBy("department", year("last_updated"), month("last_updated"))  # Time-based partitioning
 .saveAsTable(table_name))

print(f"Created table '{table_name}' with {df.count()} records")

# Z-order the table by id and salary within partitions
spark.sql(f"OPTIMIZE {table_name} ZORDER BY (id, salary)")

# Create source data for upsert (mix of updates and new records)
update_data = [(str(uuid.uuid4()), f"Updated_Name_{i}", 25 + i % 35, 35000 + (i * 1500 % 80000), f"Dept_{i % 6}", expr("current_date()")) for i in range(1000, 2000)]
source_df = spark.createDataFrame(update_data, schema)

# Optimize source data with advanced salting to handle skew
salt_factor = 40
source_df = (source_df
             .withColumn("salt", (sha1(col("id") + col("department")) % salt_factor).cast("int"))
             .repartition(40 * salt_factor, "salt", "department", year("last_updated"), month("last_updated"))
             .sortWithinPartitions("salt", "id", "salary")
             .drop("salt"))

# Cache the source DataFrame with selective caching
source_df.persist()  # Use selective persistence instead of cache()
source_df.count()  # Materialize the persistence

# Perform the merge operation
target_table = DeltaTable.forName(spark, table_name)

# Use a dynamic broadcast join threshold with a higher limit
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")
spark.conf.set("spark.sql.adaptive.autoBroadcastJoinThreshold", "256MB")

# Merge with optimized write and dynamic partition overwrite
merge_builder = (target_table.alias("target")
                 .merge(source_df.alias("source"),
                        """target.id = source.id AND 
                           target.department = source.department AND
                           year(target.last_updated) = year(source.last_updated) AND
                           month(target.last_updated) = month(source.last_updated)""")
                 .whenMatchedUpdate(
    condition="source.salary > target.salary OR source.age != target.age",
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
 .option("txnVersion", "latest")  # Use the latest transaction version for better concurrency
 .option("txnAppId", "upsert_job")  # Identify the transaction for monitoring
 .execute())

# Optimize the table after merge with advanced options
target_table.optimize().where("department IS NOT NULL").executeCompaction()

# Use deletion vectors for efficient file management with a shorter retention period
target_table.vacuum(72)  # 72 hours = 3 days

# Verify the results
result_df = spark.table(table_name)
print(f"\nFinal record count: {result_df.count()}")
print("\nSample data after upsert:")
result_df.show(10, truncate=False)

# Analyze table for better statistics with more granular analysis
spark.sql(f"ANALYZE TABLE {table_name} COMPUTE STATISTICS FOR COLUMNS id, salary, department, last_updated")

# Clean up
spark.catalog.clearCache()