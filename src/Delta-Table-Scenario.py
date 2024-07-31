# Databricks notebook source

# COMMAND ----------
from pyspark.sql.functions import col

# COMMAND ----------
# Step 1: Create initial data
data = [(i, f"Name_{i}", i * 1000) for i in range(1, 11)]
df = spark.createDataFrame(data, ["id", "name", "value"])

# Display the initial data
display(df)

# COMMAND ----------
# Step 2: Define the external location
external_path = "/mnt/your-mount-point/path/to/external/delta/table"

# COMMAND ----------
# Step 3: Create an external Delta table
spark.sql(f"""
CREATE TABLE IF NOT EXISTS external_delta_table (
    id INT,
    name STRING,
    value INT
)
USING DELTA
LOCATION '{external_path}'
""")

# COMMAND ----------
# Step 4: Write the initial data to the external table
df.write.format("delta").mode("overwrite").saveAsTable("external_delta_table")

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
delta_table = DeltaTable.forName(spark, "external_delta_table")

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

# COMMAND ----------
# Step 10: Verify that the table is external
spark.sql("DESCRIBE EXTENDED external_delta_table").show(truncate=False)