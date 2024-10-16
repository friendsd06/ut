# Databricks notebook source
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, count, when, lit
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
from IPython.display import display, HTML
import json

# Initialize Spark Session
spark = SparkSession.builder.appName("ReconTool").getOrCreate()

# Create sample datasets (you can replace these with your actual tables)
source_schema = StructType([
    StructField("customer_id", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("age", IntegerType(), True),
    StructField("city", StringType(), True),
    StructField("balance", DoubleType(), True)
])

target_schema = StructType([
    StructField("customer_id", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("age", IntegerType(), True),
    StructField("city", StringType(), True),
    StructField("balance", DoubleType(), True)
])

source_data = [
    (1, "Alice Johnson", 32, "New York", 5000.50),
    (2, "Bob Smith", 45, "Los Angeles", 7500.75),
    (3, "Charlie Brown", 28, "Chicago", 3200.25),
    (4, "Diana Ross", 39, "Houston", 6100.00),
    (5, "Edward Norton", 52, "San Francisco", 9800.50)
]

target_data = [
    (1, "Alice Johnson", 32, "New York", 5000.50),
    (2, "Bob Smith", 46, "Los Angeles", 7500.75),  # Age mismatch
    (3, "Charlie Brown", 28, "Chicago", 3250.25),  # Balance mismatch
    (4, "Diana Ross", 39, "Dallas", 6100.00),  # City mismatch
    (6, "Frank Sinatra", 65, "Las Vegas", 12000.00)  # New record in target
]

source_df = spark.createDataFrame(source_data, source_schema)
target_df = spark.createDataFrame(target_data, target_schema)

# Register the DataFrames as temporary views
source_df.createOrReplaceTempView("source_table")
target_df.createOrReplaceTempView("target_table")

# COMMAND ----------

# Function to get available tables
def get_available_tables():
    return [table.tableName for table in spark.sql("SHOW TABLES").collect()]

# Function to get columns of a table
def get_table_columns(table_name):
    return spark.table(table_name).columns

# Function to perform reconciliation
def reconcile_dataframes(source_df: DataFrame, target_df: DataFrame, columns_to_check: list) -> DataFrame:
    joined_df = source_df.join(target_df, "customer_id", "full_outer")

    conditions = [
        when(col(f"source.{c}") != col(f"target.{c}"), "Mismatch")
            .when(col(f"source.{c}").isNull() & col(f"target.{c}").isNotNull(), "Missing in Source")
            .when(col(f"source.{c}").isNotNull() & col(f"target.{c}").isNull(), "Missing in Target")
            .otherwise("Match")
        for c in columns_to_check
    ]

    return joined_df.select(
        when(col("source.customer_id").isNotNull(), col("source.customer_id"))
            .otherwise(col("target.customer_id")).alias("customer_id"),
        *conditions
    ).groupBy("customer_id").agg(
        *[count(when(col(c) == "Mismatch", 1)).alias(f"{c}_Mismatch") for c in columns_to_check],
        *[count(when(col(c) == "Missing in Source", 1)).alias(f"{c}_Missing_in_Source") for c in columns_to_check],
        *[count(when(col(c) == "Missing in Target", 1)).alias(f"{c}_Missing_in_Target") for c in columns_to_check],
        *[count(when(col(c) == "Match", 1)).alias(f"{c}_Match") for c in columns_to_check]
    )

# COMMAND ----------

# Create widgets
dbutils.widgets.dropdown("source_table", "source_table", get_available_tables())
dbutils.widgets.dropdown("target_table", "target_table", get_available_tables())

# We'll update this multiselect after table selection
dbutils.widgets.multiselect("columns_to_reconcile", "", [])

dbutils.widgets.button("update_columns", "Update Columns")
dbutils.widgets.button("run_reconciliation", "Run Reconciliation")

# COMMAND ----------

# Function to update column selection based on chosen tables
def update_column_selection():
    source_table = dbutils.widgets.get("source_table")
    target_table = dbutils.widgets.get("target_table")

    source_columns = set(get_table_columns(source_table))
    target_columns = set(get_table_columns(target_table))
    common_columns = list(source_columns.intersection(target_columns))

    dbutils.widgets.remove("columns_to_reconcile")
    dbutils.widgets.multiselect("columns_to_reconcile", "", common_columns)

# COMMAND ----------

# Main reconciliation function
def run_reconciliation():
    source_table = dbutils.widgets.get("source_table")
    target_table = dbutils.widgets.get("target_table")
    columns_to_reconcile = dbutils.widgets.get("columns_to_reconcile").split(",")

    if not columns_to_reconcile:
        print("Please select at least one column to reconcile.")
        return

    source_df = spark.table(source_table)
    target_df = spark.table(target_table)

    result_df = reconcile_dataframes(source_df, target_df, columns_to_reconcile)

    # Display results
    display(result_df)

    # Check for discrepancies
    discrepancies = result_df.filter(" OR ".join([f"{col}_Mismatch > 0 OR {col}_Missing_in_Source > 0 OR {col}_Missing_in_Target > 0" for col in columns_to_reconcile]))

    if discrepancies.count() > 0:
        print("Reconciliation completed with discrepancies. Please review the results above.")
    else:
        print("Reconciliation completed successfully. No discrepancies found.")

# COMMAND ----------

# Event handlers for button clicks
dbutils.widgets.onEvent("update_columns", update_column_selection)
dbutils.widgets.onEvent("run_reconciliation", run_reconciliation)

# COMMAND ----------

print("Reconciliation Tool is ready. Please use the widgets above to select source and target tables, update columns, and run the reconciliation.")

# COMMAND ----------

# You can add additional cells here for any extra functionality or instructions