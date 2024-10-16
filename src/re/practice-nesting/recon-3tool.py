# Databricks notebook source
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, count, when, lit
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
import ipywidgets as widgets
from IPython.display import display, HTML
import json

# Initialize Spark Session
spark = SparkSession.builder.appName("ReconTool").getOrCreate()

# ==================== Define Your Custom Datasets ====================

# Define the schema for source DataFrame
source_schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("age", IntegerType(), True),
    StructField("salary", DoubleType(), True)
])

# Define the schema for target DataFrame
target_schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("age", IntegerType(), True),
    StructField("salary", DoubleType(), True)
])

# Replace the sample data below with your own custom data
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

# Create DataFrames using the custom data
source_df = spark.createDataFrame(source_data, source_schema)
target_df = spark.createDataFrame(target_data, target_schema)

# ==================== End of Custom Dataset Definition ====================

def get_common_columns(source_df: DataFrame, target_df: DataFrame) -> list:
    """
    Retrieve the list of common columns between source and target DataFrames.
    """
    source_columns = source_df.columns
    target_columns = target_df.columns
    return list(set(source_columns) & set(target_columns))

def reconcile_dataframes(source_df: DataFrame, target_df: DataFrame, columns_to_check: list) -> DataFrame:
    """
    Reconcile two DataFrames based on specified columns.
    """
    # Assuming 'id' is the key for joining; modify if different
    key_column = "id"

    joined_df = source_df.alias("source").join(target_df.alias("target"), on=key_column, how="full_outer")

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
columns_to_reconcile = get_common_columns(source_df, target_df)
columns_widget = widgets.SelectMultiple(
    options=columns_to_reconcile,
    value=columns_to_reconcile,  # Default to all common columns selected
    description='Columns to Reconcile:',
    disabled=False
)
run_button = widgets.Button(description="Run Reconciliation")
output = widgets.Output()

reconciliation_results = []

def run_reconciliation(b):
    with output:
        output.clear_output()
        print("Running reconciliation...")
        columns_to_check = list(columns_widget.value)  # Convert to list for processing

        if not columns_to_check:
            print("Please select at least one column to reconcile.")
            return

        result_df = reconcile_dataframes(source_df, target_df, columns_to_check)

        html_table = "<table style='border-collapse: collapse; width: 100%;'>"
        html_table += "<tr style='background-color: #f2f2f2;'>"
        html_table += "<th style='border: 1px solid #ddd; padding: 8px;'>Column</th>"
        html_table += "<th style='border: 1px solid #ddd; padding: 8px;'>Matches</th>"
        html_table += "<th style='border: 1px solid #ddd; padding: 8px;'>Mismatches</th>"
        html_table += "<th style='border: 1px solid #ddd; padding: 8px;'>Missing in Source</th>"
        html_table += "<th style='border: 1px solid #ddd; padding: 8px;'>Missing in Target</th>"
        html_table += "</tr>"

        recon_summary = {}
        for c in columns_to_check:
            # Collect metrics for each column
            metrics = result_df.select(
                f"{c}_Match", f"{c}_Mismatch", f"{c}_Missing_in_Source", f"{c}_Missing_in_Target"
            ).first()
            if metrics:
                matches, mismatches, missing_source, missing_target = metrics
            else:
                matches = mismatches = missing_source = missing_target = 0

            recon_summary[c] = {
                "Matches": matches,
                "Mismatches": mismatches,
                "Missing in Source": missing_source,
                "Missing in Target": missing_target
            }

            # Build HTML table rows
            html_table += f"<tr>"
            html_table += f"<td style='border: 1px solid #ddd; padding: 8px;'>{c}</td>"
            html_table += f"<td style='border: 1px solid #ddd; padding: 8px;'>{matches}</td>"
            html_table += f"<td style='border: 1px solid #ddd; padding: 8px; background-color: {'#ffcccc' if mismatches > 0 else 'inherit'};'>{mismatches}</td>"
            html_table += f"<td style='border: 1px solid #ddd; padding: 8px; background-color: {'#ffffcc' if missing_source > 0 else 'inherit'};'>{missing_source}</td>"
            html_table += f"<td style='border: 1px solid #ddd; padding: 8px; background-color: {'#ffffcc' if missing_target > 0 else 'inherit'};'>{missing_target}</td>"
            html_table += f"</tr>"

        html_table += "</table>"
        display(HTML(html_table))

        reconciliation_results.append({
            "Columns": columns_to_check,
            "Summary": recon_summary
        })

        print("\nReconciliation completed successfully!")
        print(f"Total reconciliations performed: {len(reconciliation_results)}")

run_button.on_click(run_reconciliation)

# Display the UI
display(widgets.VBox([columns_widget, run_button, output]))

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
