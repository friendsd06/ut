from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, rand, expr
from delta.tables import DeltaTable
import uuid



This script does the following:

Creates a table named employee_table with a proper schema including id, name, age, salary, department, and last_updated date.
Inserts 1500 initial records into the table.
Creates a source DataFrame with 1000 records for the upsert operation. These include both updates to existing records and new records.
Performs a merge operation (upsert) on the target table using the source data. It updates records when the source salary is higher than the target salary, and inserts new records when there's no match.
Applies performance improvements including repartitioning, caching, and enabling optimizeWrite and autoCompact.
Verifies the results by showing the final record count and a sample of the data after the upsert.

# Initialize Spark Session
spark = SparkSession.builder.appName("UpsertExample").getOrCreate()

# Define the schema for our table
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, DateType

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

# Write data to Delta table
table_name = "employee_table"
df.write.format("delta").mode("overwrite").saveAsTable(table_name)

print(f"Created table '{table_name}' with {df.count()} records")

# Create source data for upsert (mix of updates and new records)
update_data = [(str(uuid.uuid4()), f"Updated_Name_{i}", 25 + i % 35, 35000 + (i * 1500 % 80000), f"Dept_{i % 6}", expr("current_date()")) for i in range(1000, 2000)]
source_df = spark.createDataFrame(update_data, schema)

# Perform the merge operation
target_table = DeltaTable.forName(spark, table_name)

target_table.alias("target").merge(
    source_df.alias("source"),
    "target.id = source.id"
).whenMatchedUpdate(
    condition = "source.salary > target.salary",
    set = {
        "name": "source.name",
        "age": "source.age",
        "salary": "source.salary",
        "department": "source.department",
        "last_updated": "source.last_updated"
    }
).whenNotMatchedInsert(
    values = {
        "id": "source.id",
        "name": "source.name",
        "age": "source.age",
        "salary": "source.salary",
        "department": "source.department",
        "last_updated": "source.last_updated"
    }
).execute()

# Performance improvements
# 1. Repartition source DataFrame
source_df = source_df.repartition(20, "id")

# 2. Cache the source DataFrame
source_df.cache()

# 3. Enable optimizeWrite and autoCompact
spark.conf.set("spark.databricks.delta.optimizeWrite.enabled", "true")
spark.conf.set("spark.databricks.delta.autoCompact.enabled", "true")

# Verify the results
result_df = spark.table(table_name)
print(f"\nFinal record count: {result_df.count()}")
print("\nSample data after upsert:")
result_df.show(10, truncate=False)

# Clean up
spark.catalog.clearCache()