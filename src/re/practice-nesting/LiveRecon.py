# Databricks notebook source
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, count, when, lit
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
from IPython.display import display, HTML
import json

# Initialize Spark Session
spark = SparkSession.builder.appName("ReconDashboard").getOrCreate()

# Create sample datasets
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

# HTML template for the dashboard
html_template = """
<html>
<head>
    <style>
        body { font-family: Arial, sans-serif; margin: 0; padding: 0; display: flex; flex-direction: column; height: 100vh; }
        #header { background-color: #4CAF50; color: white; padding: 10px; text-align: center; }
        #main-container { display: flex; flex: 1; }
        #sidebar { width: 200px; background-color: #f1f1f1; padding: 20px; }
        #content { flex: 1; padding: 20px; overflow: auto; }
        #right-sidebar { width: 200px; background-color: #f1f1f1; padding: 20px; }
        button { margin: 10px 0; padding: 10px; width: 100%; }
        table { border-collapse: collapse; width: 100%; }
        th, td { border: 1px solid #ddd; padding: 8px; text-align: left; }
        th { background-color: #4CAF50; color: white; }
        .success-message { background-color: #4CAF50; color: white; padding: 10px; margin: 10px 0; }
        .failure-message { background-color: #f44336; color: white; padding: 10px; margin: 10px 0; }
    </style>
</head>
<body>
    <div id="header">
        <h1>Data Reconciliation Dashboard</h1>
    </div>
    <div id="main-container">
        <div id="sidebar">
            <h3>Controls</h3>
            <button onclick="runReconciliation()">Run Reconciliation</button>
            <button onclick="showAllResults()">Show All Results</button>
        </div>
        <div id="content">
            <h2>Reconciliation Results</h2>
            <div id="results"></div>
        </div>
        <div id="right-sidebar">
            <h3>Statistics</h3>
            <div id="stats"></div>
        </div>
    </div>

    <script>
        var reconciliationResults = [];

        function runReconciliation() {
            var result = JSON.parse(IPython.notebook.kernel.execute('reconcile_dataframes(source_df, target_df, ["name", "age", "city", "balance"]).toJSON().collect()'));
            displayResults(result);
            updateStats();
        }

        function displayResults(result) {
            var content = '<table><tr><th>Customer ID</th><th>Name</th><th>Age</th><th>City</th><th>Balance</th></tr>';
            result.forEach(function(row) {
                content += '<tr>';
                content += '<td>' + row.customer_id + '</td>';
                content += '<td class="' + getStatusClass(row.name_Mismatch, row.name_Missing_in_Source, row.name_Missing_in_Target) + '">' + getStatus(row.name_Mismatch, row.name_Missing_in_Source, row.name_Missing_in_Target) + '</td>';
                content += '<td class="' + getStatusClass(row.age_Mismatch, row.age_Missing_in_Source, row.age_Missing_in_Target) + '">' + getStatus(row.age_Mismatch, row.age_Missing_in_Source, row.age_Missing_in_Target) + '</td>';
                content += '<td class="' + getStatusClass(row.city_Mismatch, row.city_Missing_in_Source, row.city_Missing_in_Target) + '">' + getStatus(row.city_Mismatch, row.city_Missing_in_Source, row.city_Missing_in_Target) + '</td>';
                content += '<td class="' + getStatusClass(row.balance_Mismatch, row.balance_Missing_in_Source, row.balance_Missing_in_Target) + '">' + getStatus(row.balance_Mismatch, row.balance_Missing_in_Source, row.balance_Missing_in_Target) + '</td>';
                content += '</tr>';
            });
            content += '</table>';
            document.getElementById('results').innerHTML = content;
            reconciliationResults.push(result);
            
            var message = (result.some(row => 
                row.name_Mismatch > 0 || row.age_Mismatch > 0 || row.city_Mismatch > 0 || row.balance_Mismatch > 0 ||
                row.name_Missing_in_Source > 0 || row.age_Missing_in_Source > 0 || row.city_Missing_in_Source > 0 || row.balance_Missing_in_Source > 0 ||
                row.name_Missing_in_Target > 0 || row.age_Missing_in_Target > 0 || row.city_Missing_in_Target > 0 || row.balance_Missing_in_Target > 0
            )) 
                ? '<div class="failure-message">Reconciliation completed with discrepancies. Please review the results.</div>'
                : '<div class="success-message">Reconciliation completed successfully. No discrepancies found.</div>';
            document.getElementById('results').innerHTML = message + content;
        }

        function getStatus(mismatch, missingSource, missingTarget) {
            if (mismatch > 0) return 'Mismatch';
            if (missingSource > 0) return 'Missing in Source';
            if (missingTarget > 0) return 'Missing in Target';
            return 'Match';
        }

        function getStatusClass(mismatch, missingSource, missingTarget) {
            if (mismatch > 0) return 'mismatch';
            if (missingSource > 0 || missingTarget > 0) return 'missing';
            return 'match';
        }

        function updateStats() {
            var stats = {
                totalReconciliations: reconciliationResults.length,
                totalRecords: reconciliationResults[reconciliationResults.length - 1].length,
                mismatches: 0,
                missingInSource: 0,
                missingInTarget: 0
            };
            
            reconciliationResults[reconciliationResults.length - 1].forEach(function(row) {
                if (row.name_Mismatch > 0 || row.age_Mismatch > 0 || row.city_Mismatch > 0 || row.balance_Mismatch > 0) stats.mismatches++;
                if (row.name_Missing_in_Source > 0 || row.age_Missing_in_Source > 0 || row.city_Missing_in_Source > 0 || row.balance_Missing_in_Source > 0) stats.missingInSource++;
                if (row.name_Missing_in_Target > 0 || row.age_Missing_in_Target > 0 || row.city_Missing_in_Target > 0 || row.balance_Missing_in_Target > 0) stats.missingInTarget++;
            });
            
            var statsContent = '<p>Total Reconciliations: ' + stats.totalReconciliations + '</p>';
            statsContent += '<p>Total Records: ' + stats.totalRecords + '</p>';
            statsContent += '<p>Mismatches: ' + stats.mismatches + '</p>';
            statsContent += '<p>Missing in Source: ' + stats.missingInSource + '</p>';
            statsContent += '<p>Missing in Target: ' + stats.missingInTarget + '</p>';
            
            document.getElementById('stats').innerHTML = statsContent;
        }

        function showAllResults() {
            var content = '<h3>All Reconciliation Results</h3>';
            reconciliationResults.forEach(function(result, index) {
                content += '<h4>Reconciliation #' + (index + 1) + '</h4>';
                content += '<table><tr><th>Customer ID</th><th>Name</th><th>Age</th><th>City</th><th>Balance</th></tr>';
                result.forEach(function(row) {
                    content += '<tr>';
                    content += '<td>' + row.customer_id + '</td>';
                    content += '<td class="' + getStatusClass(row.name_Mismatch, row.name_Missing_in_Source, row.name_Missing_in_Target) + '">' + getStatus(row.name_Mismatch, row.name_Missing_in_Source, row.name_Missing_in_Target) + '</td>';
                    content += '<td class="' + getStatusClass(row.age_Mismatch, row.age_Missing_in_Source, row.age_Missing_in_Target) + '">' + getStatus(row.age_Mismatch, row.age_Missing_in_Source, row.age_Missing_in_Target) + '</td>';
                    content += '<td class="' + getStatusClass(row.city_Mismatch, row.city_Missing_in_Source, row.city_Missing_in_Target) + '">' + getStatus(row.city_Mismatch, row.city_Missing_in_Source, row.city_Missing_in_Target) + '</td>';
                    content += '<td class="' + getStatusClass(row.balance_Mismatch, row.balance_Missing_in_Source, row.balance_Missing_in_Target) + '">' + getStatus(row.balance_Mismatch, row.balance_Missing_in_Source, row.balance_Missing_in_Target) + '</td>';
                    content += '</tr>';
                });
                content += '</table>';
            });
            document.getElementById('results').innerHTML = content;
        }
    </script>
</body>
</html>
"""

# Display the dashboard
displayHTML(html_template)

# COMMAND ----------

# You can add additional cells here for any extra functionality or instructions