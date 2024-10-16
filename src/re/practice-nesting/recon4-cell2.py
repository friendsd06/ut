# =========================================
# Cell 2: Backend Logic and Functions (Restricted to Developers)
# =========================================

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, count, when
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
import json

# Initialize Spark Session
spark = SparkSession.builder.appName("ReconTool").getOrCreate()

# ==================== Define Your Custom Datasets ====================

# Example schemas and data. Replace with actual data loading logic.
def load_data(environment, dataset, cobdate):
    """
    Load source and target DataFrames based on environment, dataset, and cobdate.
    This function should be updated to fetch data from actual sources.
    """
    # Placeholder schemas and data
    if dataset == 'Dataset1':
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

    elif dataset == 'Dataset2':
        # Define schemas and data for Dataset2
        pass
    else:
        # Define schemas and data for Dataset3
        pass

    return source_df, target_df

# ==================== Reconciliation Function ====================

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

    result_df = joined_df.select(
        *[when(col(f"source.{c}").isNotNull(), col(f"source.{c}"))
              .otherwise(col(f"target.{c}")).alias(c) for c in columns_to_check],
        *conditions
    ).groupBy(*columns_to_check).agg(
        *[count(when(col(c) == "Mismatch", 1)).alias(f"{c}_Mismatch") for c in columns_to_check],
        *[count(when(col(c) == "Missing in Source", 1)).alias(f"{c}_Missing_in_Source") for c in columns_to_check],
        *[count(when(col(c) == "Missing in Target", 1)).alias(f"{c}_Missing_in_Target") for c in columns_to_check],
        *[count(when(col(c) == "Match", 1)).alias(f"{c}_Match") for c in columns_to_check]
    )

    return result_df

# ==================== Reconciliation Execution ====================

def perform_reconciliation(environment, dataset, cobdate, columns_to_check):
    """
    Perform the reconciliation process based on user inputs.
    """
    # Load data based on inputs
    source_df, target_df = load_data(environment, dataset, cobdate)

    # Perform reconciliation
    result_df = reconcile_dataframes(source_df, target_df, columns_to_check)

    # Display results
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

    # Optionally, store reconciliation results
    # You can extend this to save results to a database or file system
