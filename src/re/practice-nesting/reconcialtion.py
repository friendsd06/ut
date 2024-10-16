# Databricks notebook source
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, count, when, lit
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
import ipywidgets as widgets
from IPython.display import display, HTML
import json

# Initialize Spark Session
spark = SparkSession.builder.appName("ReconTool").getOrCreate()

# Create sample datasets
source_schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("age", IntegerType(), True),
    StructField("salary", DoubleType(), True)
])

target_schema = StructType([
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

source_df = spark.createDataFrame(source_data, source_schema)
target_df = spark.createDataFrame(target_data, target_schema)

# Register the DataFrames as temporary views
source_df.createOrReplaceTempView("source_table")
target_df.createOrReplaceTempView("target_table")

def get_available_tables():
    return spark.sql("SHOW TABLES").select("tableName").rdd.flatMap(lambda x: x).collect()

def get_table_columns(table_name):
    return spark.table(table_name).columns

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
tables = get_available_tables()
source_table_widget = widgets.Dropdown(options=tables, description='Source Table:')
target_table_widget = widgets.Dropdown(options=tables, description='Target Table:')
columns_widget = widgets.SelectMultiple(options=[], description='Columns to Reconcile:')
run_button = widgets.Button(description="Run Reconciliation")
output = widgets.Output()

def update_columns(*args):
    source_columns = get_table_columns(source_table_widget.value)
    target_columns = get_table_columns(target_table_widget.value)
    common_columns = list(set(source_columns) & set(target_columns))
    columns_widget.options = common_columns

source_table_widget.observe(update_columns, names='value')
target_table_widget.observe(update_columns, names='value')

reconciliation_results = []

def run_reconciliation(b):
    with output:
        output.clear_output()
        print("Running reconciliation...")
        source_df = spark.table(source_table_widget.value)
        target_df = spark.table(target_table_widget.value)
        columns_to_check = columns_widget.value

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
        display(HTML(html_table))

        reconciliation_results.append({
            "Source": source_table_widget.value,
            "Target": target_table_widget.value,
            "Columns": columns_to_check,
            "Summary": recon_summary
        })

        print("\nReconciliation completed successfully!")
        print(f"Total reconciliations performed: {len(reconciliation_results)}")

run_button.on_click(run_reconciliation)

# Display the UI
display(source_table_widget, target_table_widget, columns_widget, run_button, output)

# Add a button to show all reconciliation results
show_all_results_button = widgets.Button(description="Show All Reconciliation Results")
all_results_output = widgets.Output()

def show_all_results(b):
    with all_results_output:
        all_results_output.clear_output()
        if not reconciliation_results:
            print("No reconciliations have been performed yet.")
        else:
            for idx, result in enumerate(reconciliation_results, 1):
                print(f"\nReconciliation #{idx}")
                print(f"Source: {result['Source']}")
                print(f"Target: {result['Target']}")
                print(f"Columns: {', '.join(result['Columns'])}")
                print("Summary:")
                for col, summary in result['Summary'].items():
                    print(f"  {col}:")
                    for metric, value in summary.items():
                        print(f"    {metric}: {value}")
                print("-" * 50)

show_all_results_button.on_click(show_all_results)

display(show_all_results_button, all_results_output)

# COMMAND ----------

# You can add additional cells here for any extra functionality or instructions