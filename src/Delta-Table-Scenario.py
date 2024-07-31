# COMMAND ----------
# Step 1: Set up the Spark session
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, col
from delta.tables import DeltaTable

spark = SparkSession.builder.getOrCreate()

# COMMAND ----------
# Step 2: Create initial data
data = [(i, f"Name_{i}", i * 1000) for i in range(1, 11)]
df = spark.createDataFrame(data, ["id", "name", "value"])

# Display the initial data
display(df)

# COMMAND ----------
# Step 3: Write the data to an external location (e.g., S3)
external_path = "s3a://your-bucket/path/to/external/delta/table"

# Write the data using Delta format
df.write.format("delta").mode("overwrite").save(external_path)

# COMMAND ----------
# Step 4: Create an external table in the metastore
spark.sql(f"""
CREATE TABLE external_delta_table
USING DELTA
LOCATION '{external_path}'
""")

# COMMAND ----------
# Step 5: Verify the data
delta_df = spark.table("external_delta_table")
display(delta_df)

# COMMAND ----------
# Step 6: Prepare updates for 5 records
updates = [
    (1, "Updated_Name_1", 1500),
    (3, "Updated_Name_3", 3500),
    (5, "Updated_Name_5", 5500),
    (7, "Updated_Name_7", 7500),
    (9, "Updated_Name_9", 9500)
]

update_df = spark.createDataFrame(updates, ["id", "name", "value"])

# COMMAND ----------
# Step 7: Perform the merge operation
delta_table = DeltaTable.forPath(spark, external_path)

# Perform merge operation
delta_table.alias("original") \
    .merge(
    update_df.alias("updates"),
    "original.id = updates.id"
) \
    .whenMatchedUpdate(set =
{
    "name": "updates.name",
    "value": "updates.value"
}
) \
    .execute()

# COMMAND ----------
# Step 8: Verify the updates
updated_df = spark.table("external_delta_table")
display(updated_df.orderBy("id"))

# COMMAND ----------
# Step 9: Check the history of the Delta table
display(delta_table.history())