# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from delta.tables import *
import random
from datetime import datetime, timedelta

# COMMAND ----------

# Setup Spark Session with extensive Delta configurations
spark = SparkSession.builder \
    .appName("DeltaConfigDemo") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.databricks.delta.optimizeWrite.enabled", "true") \
    .config("spark.databricks.delta.autoCompact.enabled", "true") \
    .config("spark.databricks.delta.autoCompact.minNumFiles", "50") \
    .config("spark.databricks.delta.optimizeWrite.binSize", "256m") \
    .config("spark.delta.snapshotPartitions", "50") \
    .config("spark.databricks.delta.properties.defaults.enableChangeDataFeed", "true") \
    .config("spark.databricks.delta.schema.autoMerge.enabled", "true") \
    .config("spark.databricks.delta.merge.optimizeInsertOnlyMerge.enabled", "true") \
    .config("spark.databricks.delta.commitInfo.userMetadata", "Delta Config Demo") \
    .config("spark.databricks.delta.stats.collect", "true") \
    .config("spark.sql.shuffle.partitions", "200") \
    .getOrCreate()

# Display current Delta configurations
print("Current Delta Configurations:")
for conf in spark.conf.get("spark.databricks.delta").split(","):
    print(conf)

# COMMAND ----------

# Define schema for our dataset
schema = StructType([
    StructField("id", IntegerType(), False),
    StructField("name", StringType(), True),
    StructField("department", StringType(), True),
    StructField("salary", DoubleType(), True),
    StructField("hire_date", DateType(), True)
])

# Function to generate sample data
def generate_data(num_records):
    def random_date():
        start_date = datetime(2010, 1, 1)
        end_date = datetime.now()
        time_between = end_date - start_date
        days_between = time_between.days
        random_days = random.randrange(days_between)
        return start_date + timedelta(days=random_days)

    data = [(i,
             f"Employee_{i}",
             random.choice(["HR", "IT", "Finance", "Marketing", "Sales"]),
             round(random.uniform(30000, 150000), 2),
             random_date().date())
            for i in range(1, num_records + 1)]
    return spark.createDataFrame(data, schema)

# COMMAND ----------

# S3 path for our Delta table
s3_delta_path = "/mnt/your-s3-mount/delta_config_demo"

# Generate and write initial dataset
print("Generating and writing initial dataset...")
initial_df = generate_data(100000)
initial_df.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .partitionBy("department") \
    .save(s3_delta_path)

print(f"Initial dataset written to {s3_delta_path}")

# COMMAND ----------

# Function to examine table details
def examine_table_details(path):
    print("\n--- Table Details ---")
    display(spark.sql(f"DESCRIBE DETAIL delta.`{path}`"))

    print("\n--- Table History ---")
    display(spark.sql(f"DESCRIBE HISTORY delta.`{path}`"))

examine_table_details(s3_delta_path)

# COMMAND ----------

# Perform a read operation to see partition elimination in action
print("Reading data with partition filter:")
filtered_df = spark.read.format("delta").load(s3_delta_path).filter(col("department") == "IT")
filtered_df.explain()

# COMMAND ----------

# Append operation with auto optimize
print("Appending data with auto optimize...")
append_df = generate_data(50000)
append_df.write \
    .format("delta") \
    .mode("append") \
    .option("mergeSchema", "true") \
    .save(s3_delta_path)

examine_table_details(s3_delta_path)

# COMMAND ----------

# Merge operation
print("Performing merge operation...")
merge_df = generate_data(10000)
delta_table = DeltaTable.forPath(spark, s3_delta_path)

delta_table.alias("target").merge(
    merge_df.alias("source"),
    "target.id = source.id"
).whenMatchedUpdate(set =
{
    "salary": "source.salary",
    "department": "source.department"
}
).whenNotMatchedInsert(values =
{
    "id": "source.id",
    "name": "source.name",
    "department": "source.department",
    "salary": "source.salary",
    "hire_date": "source.hire_date"
}
).execute()

examine_table_details(s3_delta_path)

# COMMAND ----------

# Optimize table
print("Optimizing table...")
delta_table.optimize().executeCompaction()

# Z-order by id and hire_date
print("Z-ordering by id and hire_date...")
delta_table.optimize().executeZOrderBy("id", "hire_date")

examine_table_details(s3_delta_path)

# COMMAND ----------

# Vacuum the table
print("Vacuuming table...")
delta_table.vacuum(168)  # 7 days retention

examine_table_details(s3_delta_path)

# COMMAND ----------

# Time travel query
print("Performing time travel query...")
version_1_df = spark.read.format("delta").option("versionAsOf", 1).load(s3_delta_path)
print(f"Record count at version 1: {version_1_df.count()}")

# COMMAND ----------

# Generate Change Data Feed
print("Generating Change Data Feed...")
cdf_df = spark.read.format("delta") \
    .option("readChangeFeed", "true") \
    .option("startingVersion", 1) \
    .load(s3_delta_path)

print("Change Data Feed sample:")
display(cdf_df.limit(10))

# COMMAND ----------

# Final query to demonstrate performance
print("Performing final complex query...")
final_query_result = spark.read.format("delta").load(s3_delta_path) \
    .groupBy("department") \
    .agg(
    count("id").alias("employee_count"),
    avg("salary").alias("avg_salary"),
    min("hire_date").alias("earliest_hire"),
    max("hire_date").alias("latest_hire")
) \
    .orderBy(desc("avg_salary"))

display(final_query_result)