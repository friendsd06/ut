# Databricks notebook source
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, count, when, lit
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
from IPython.display import display, HTML

# Initialize Spark Session
spark = SparkSession.builder.appName("ReconTool").getOrCreate()

# Create sample datasets
schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("age", IntegerType(), True),
    StructField("salary", DoubleType(), True)
])

source_data = [
    (1, "Alice", 30, 50000.0),
    (2, "Bob", 35, 60000.0),
    (3, "Charlie", 40, 70000.0),
    (4, "David", 45, 80000.0),
    (5, "Eve", 50, 90000.0)
]

target_data = [
    (1, "Alice", 30, 50000.0),
    (2, "Bob", 36, 60000.0),  # Age mismatch
    (3, "Charlie", 40, 75000.0),  # Salary mismatch
    (4, "David", 45, 80000.0),
    (6, "Frank", 55, 95000.0)  # New record in target
]

source_df = spark.createDataFrame(source_data, schema)
target_df = spark.createDataFrame(target_data, schema)

def reconcile_dataframes(source_df: DataFrame, target_df: DataFrame, columns_to_check: list) -> DataFrame:
    joined_df = source_df.join(target_df, columns_to_check, "full_outer")

    conditions = [
        when(col(f"source.{c}") != col(f"target.{c}"), "Mismatch")
            .when(col(f"source.{c}").isNull() & col(f"target.{c}").isNotNull(), "Missing in Source")
            .when(col(f"source.{c}").isNotNull() & col(f"target.{c}").isNull(), "Missing in Target")
            .otherwise("Match")
        for c in columns_to_check
    ]

    return joined_df.select(
        *[when(col(f"source.{c}").isNotNull(), col(f"source.{c}"))
              .otherwise(col(f"target.{c}")).alias(c) for c in columns_to_check],
        *conditions
    ).groupBy(*columns_to_check).agg(
        *[count(when(col(c) == "Mismatch", 1)).alias(f"{c}_Mismatch") for c in columns_to_check],
        *[count(when(col(c) == "Missing in Source", 1)).alias(f"{c}_Missing_in_Source") for c in columns_to_check],
        *[count(when(col(c) == "Missing in Target", 1)).alias(f"{c}_Missing_in_Target") for c in columns_to_check],
        *[count(when(col(c) == "Match", 1)).alias(f"{c}_Match") for c in columns_to_check]
    )

# Create widgets for user input
columns = source_df.columns
dbutils.widgets.multiselect("columns_to_reconcile", defaultValue=columns[0], choices=columns, label="Columns to Reconcile")
dbutils.widgets.dropdown("action", defaultValue="Select Action", choices=["Select Action", "Run Reconciliation", "Show All Results"], label="Action")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Reconciliation Tool
# MAGIC Use the widgets above to select columns and run the reconciliation.

# COMMAND ----------

reconciliation_results = []

def run_reconciliation():
    columns_to_check = dbutils.widgets.get("columns_to_reconcile").split(",")

    if not columns_to_check:
        print("Please select at least one column to reconcile.")
        return

    result_df = reconcile_dataframes(source_df, target_df, columns_to_check)

    html_table = "<table style='border-collapse: collapse; width: 100%;'>"
    html_table += "<tr style='background-color: #f2f2f2;'><th style='border: 1px solid #ddd; padding: 8px;'>Column</th><th style='border: 1px solid #ddd; padding: 8px;'>Matches</th><th style='border: 1px solid #ddd; padding: 8px;'>Mismatches</th><th style='border: 1px solid #ddd; padding: 8px;'>Missing in Source</th><th style='border: 1px solid #ddd; padding: 8px;'>Missing in Target</th></tr>"

    recon_summary = {}
    for col in columns_to_check:
        matches = result_df.select(f"{col}_Match").first()[0]
        mismatches = result_df.select(f"{col}_Mismatch").first()[0]
        missing_source = result_df.select(f"{col}_Missing_in_Source").first()[0]
        missing_target = result_df.select(f"{col}_Missing_in_Target").first()[0]

        recon_summary[col] = {
            "Matches": matches,
            "Mismatches": mismatches,
            "Missing in Source": missing_source,
            "Missing in Target": missing_target
        }

        html_table += f"<tr><td style='border: 1px solid #ddd; padding: 8px;'>{col}</td>"
        html_table += f"<td style='border: 1px solid #ddd; padding: 8px;'>{matches}</td>"
        html_table += f"<td style='border: 1px solid #ddd; padding: 8px; background-color: {'#ffcccc' if mismatches > 0 else 'inherit'};'>{mismatches}</td>"
        html_table += f"<td style='border: 1px solid #ddd; padding: 8px; background-color: {'#ffffcc' if missing_source > 0 else 'inherit'};'>{missing_source}</td>"
        html_table += f"<td style='border: 1px solid #ddd; padding: 8px; background-color: {'#ffffcc' if missing_target > 0 else 'inherit'};'>{missing_target}</td></tr>"

    html_table += "</table>"
    displayHTML(html_table)

    reconciliation_results.append({
        "Columns": columns_to_check,
        "Summary": recon_summary
    })

    print("\nReconciliation completed successfully!")
    print(f"Total reconciliations performed: {len(reconciliation_results)}")

def show_all_results():
    if not reconciliation_results:
        print("No reconciliations have been performed yet.")
    else:
        for idx, result in enumerate(reconciliation_results, 1):
            print(f"\nReconciliation #{idx}")
            print(f"Columns: {', '.join(result['Columns'])}")
            print("Summary:")
            for col, summary in result['Summary'].items():
                print(f"  {col}:")
                for metric, value in summary.items():
                    print(f"    {metric}: {value}")
            print("-" * 50)

def handle_action(action):
    if action == "Run Reconciliation":
        run_reconciliation()
    elif action == "Show All Results":
        show_all_results()

# Set up the action handler
dbutils.widgets.onEvent("action", handle_action)

print("Reconciliation Tool is ready. Please select columns and choose an action from the dropdown.")

# COMMAND ----------

# MAGIC %md
# MAGIC ### How to Use
# MAGIC 1. Select the columns you want to reconcile from the "Columns to Reconcile" dropdown.
# MAGIC 2. Choose "Run Reconciliation" from the Action dropdown to perform the reconciliation.
# MAGIC 3. View the results in the table that appears below.
# MAGIC 4. To see a summary of all reconciliations performed, choose "Show All Results" from the Action dropdown.