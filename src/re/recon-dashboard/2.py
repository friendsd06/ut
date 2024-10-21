# Databricks notebook source
# MAGIC %md
# MAGIC # 🌟 Advanced Data Reconciliation Dashboard
# MAGIC
# MAGIC Welcome to the **Advanced Data Reconciliation Dashboard**! This tool allows you to perform in-depth comparisons between source and target datasets with a multitude of features for a rich analytical experience.

# COMMAND ----------

# Import necessary libraries
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    concat_ws, col, lit, when, countDistinct, approx_count_distinct,
    mean, stddev, min, max, expr, levenshtein, abs
)
from pyspark.sql.types import *
import matplotlib.pyplot as plt
import pandas as pd
import numpy as np

# COMMAND ----------

# Create sample data for source and target datasets

# Feature 1: Sample Data Generation with Realistic Data

source_data = [
    (1, 'Alice', 28, 'New York', 70000.0),
    (2, 'Bob', 35, 'Los Angeles', 80000.0),
    (3, 'Charlie', 25, 'Chicago', 65000.0),
    (4, 'Diana', 30, 'Houston', 72000.0),
    (5, 'Ethan', 45, 'Phoenix', 90000.0)
]

target_data = [
    (1, 'Alice', 28, 'New York', 70000.0),
    (2, 'Bob', 36, 'Los Angeles', 80000.0),  # Age mismatch
    (3, 'Charlie', 25, 'Chicago', 65000.0),
    (4, 'Diana', 30, 'Houston', 72000.0),
    (6, 'Fiona', 29, 'Philadelphia', 68000.0)  # New record
]

schema = StructType([
    StructField('ID', IntegerType(), True),
    StructField('Name', StringType(), True),
    StructField('Age', IntegerType(), True),
    StructField('City', StringType(), True),
    StructField('Salary', DoubleType(), True)
])

source_df = spark.createDataFrame(source_data, schema)
target_df = spark.createDataFrame(target_data, schema)

# COMMAND ----------

# Feature 2: Widgets for Selecting Datasets (Using Sample Data)
dbutils.widgets.dropdown('source_dataset', 'Sample Source', ['Sample Source'], '🔹 Select Source Dataset')
dbutils.widgets.dropdown('target_dataset', 'Sample Target', ['Sample Target'], '🔸 Select Target Dataset')

# COMMAND ----------

# Feature 3: Custom PySpark SQL Queries for Source and Target
dbutils.widgets.text('source_query', '', '📝 Source Query (Optional)')
dbutils.widgets.text('target_query', '', '📝 Target Query (Optional)')

# Feature 4: Attribute Selection using text input
attribute_choices = [f.name for f in source_df.schema.fields]
default_attributes = 'ID,Name'  # Default attributes as a comma-separated string
dbutils.widgets.text('attributes', default_attributes, '🔎 Enter Attributes (Comma-Separated)')

# Feature 5: Reconciliation Option Selection
dbutils.widgets.dropdown('recon_option', 'All', ['Match', 'Mismatch', 'All'], '⚙️ Reconciliation Option')

# Feature 6: Numeric Comparison Threshold
dbutils.widgets.text('numeric_threshold', '0', '📏 Numeric Comparison Threshold')

# Feature 7: Enable Fuzzy Matching
dbutils.widgets.dropdown('fuzzy_matching', 'No', ['Yes', 'No'], '🔄 Enable Fuzzy Matching')

# Feature 8: Export Options
dbutils.widgets.dropdown('export_option', 'None', ['None', 'CSV', 'Excel'], '💾 Export Results')

# Feature 9: Visualization Type
dbutils.widgets.dropdown('visualization_type', 'Bar Chart', ['Bar Chart', 'Pie Chart'], '📊 Visualization Type')

# COMMAND ----------

# Retrieve widget values
source_query = dbutils.widgets.get('source_query')
target_query = dbutils.widgets.get('target_query')
attributes_input = dbutils.widgets.get('attributes')

if attributes_input.strip():
    attributes = [attr.strip() for attr in attributes_input.split(',') if attr.strip() in attribute_choices]
else:
    attributes = attribute_choices  # Use all columns if none are provided

recon_option = dbutils.widgets.get('recon_option')
numeric_threshold = float(dbutils.widgets.get('numeric_threshold'))
fuzzy_matching = dbutils.widgets.get('fuzzy_matching') == 'Yes'
export_option = dbutils.widgets.get('export_option')
visualization_type = dbutils.widgets.get('visualization_type')

# COMMAND ----------

# Apply custom queries if provided
if source_query.strip():
    source_df = spark.sql(source_query)
if target_query.strip():
    target_df = spark.sql(target_query)

# COMMAND ----------

# Feature 10: Advanced Data Profiling
def data_profiling(df, df_name):
    profiling_df = df.describe()
    displayHTML(f"<h3>Data Profiling for {df_name}</h3>")
    display(profiling_df)

data_profiling(source_df, 'Source Dataset')
data_profiling(target_df, 'Target Dataset')

# COMMAND ----------

# Feature 11: Attribute Handling
if attributes:
    source_df = source_df.select(*attributes)
    target_df = target_df.select(*attributes)
else:
    attributes = source_df.columns  # Use all columns if none are selected

# COMMAND ----------

# Feature 12: Customizable Thresholds for Numeric Comparisons
def compare_numeric_columns(col_name):
    return when(
        abs(col(f'src.{col_name}') - col(f'tgt.{col_name}')) <= numeric_threshold,
        True
    ).otherwise(False)

# COMMAND ----------

# Feature 13: Fuzzy Matching for String Comparisons
def compare_string_columns(col_name):
    if fuzzy_matching:
        return when(
            levenshtein(col(f'src.{col_name}'), col(f'tgt.{col_name}')) <= 2,
            True
        ).otherwise(False)
    else:
        return when(
            col(f'src.{col_name}') == col(f'tgt.{col_name}'),
            True
        ).otherwise(False)

# COMMAND ----------

# Feature 14: Key Generation
key_cols = ['ID']  # Assuming 'ID' is the primary key
source_df = source_df.withColumn('key', concat_ws('_', *key_cols))
target_df = target_df.withColumn('key', concat_ws('_', *key_cols))

# COMMAND ----------

# Perform join
joined_df = source_df.alias('src').join(
    target_df.alias('tgt'),
    on='key',
    how='full_outer'
)

# COMMAND ----------

# Feature 15: Advanced Reconciliation Logic
status_conditions = []
for col_name in attributes:
    if col_name in key_cols:
        continue
    data_type = dict(source_df.dtypes)[col_name]
    if data_type in ['int', 'bigint', 'double', 'float']:
        status_conditions.append(compare_numeric_columns(col_name).alias(f'{col_name}_match'))
    elif data_type == 'string':
        status_conditions.append(compare_string_columns(col_name).alias(f'{col_name}_match'))
    else:
        status_conditions.append((col(f'src.{col_name}') == col(f'tgt.{col_name}')).alias(f'{col_name}_match'))

# Add key and status columns
joined_df = joined_df.select(
    'key',
    *[col(f'src.{col_name}').alias(f'src_{col_name}') for col_name in attributes],
    *[col(f'tgt.{col_name}').alias(f'tgt_{col_name}') for col_name in attributes],
    *status_conditions
)

# COMMAND ----------

# Feature 16: Status Determination
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

def determine_status(*cols):
    matches = cols
    if all(match == True for match in matches):
        return '✅ Match'
    elif any(match == False for match in matches):
        return '❌ Mismatch'
    else:
        return '🔍 Partial Match'

match_cols = [f'{col}_match' for col in attributes if col not in key_cols]
status_udf = udf(determine_status, StringType())
joined_df = joined_df.withColumn('status', status_udf(*match_cols))

# COMMAND ----------

# Feature 17: Filtering Based on Recon Option
if recon_option == 'Match':
    result_df = joined_df.filter(col('status') == '✅ Match')
elif recon_option == 'Mismatch':
    result_df = joined_df.filter(col('status') == '❌ Mismatch')
else:
    result_df = joined_df

# COMMAND ----------

# Feature 18: Highlighting Differences
def highlight_differences(df):
    diff_columns = []
    for col_name in attributes:
        if col_name in key_cols:
            continue
        df = df.withColumn(
            f'{col_name}_diff',
            when(col(f'{col_name}_match') == False, '🔴').otherwise('🟢')
        )
        diff_columns.append(f'{col_name}_diff')
    return df, diff_columns

result_df, diff_columns = highlight_differences(result_df)

# COMMAND ----------

# Feature 19: Visualization
# Generate summary statistics for visualization
summary_df = result_df.groupBy('status').count().toPandas()

# Visualization based on user selection
if not summary_df.empty:
    if visualization_type == 'Bar Chart':
        plt.figure(figsize=(8,6))
        plt.bar(summary_df['status'], summary_df['count'], color=['green', 'red', 'orange'])
        plt.title('Reconciliation Summary')
        plt.xlabel('Status')
        plt.ylabel('Record Count')
        plt.xticks(rotation=45)
        plt.grid(axis='y')
        display(plt.gcf())
    elif visualization_type == 'Pie Chart':
        plt.figure(figsize=(6,6))
        plt.pie(summary_df['count'], labels=summary_df['status'], autopct='%1.1f%%', colors=['green', 'red', 'orange'])
        plt.title('Reconciliation Summary')
        display(plt.gcf())
else:
    displayHTML("<h3>No data available for visualization.</h3>")

# COMMAND ----------

# Feature 20: Drill-Down Capability
displayHTML("<h3>Drill-Down into Reconciliation Results</h3>")
display(result_df.select('key', *[f'src_{col}' for col in attributes], *[f'tgt_{col}' for col in attributes], 'status', *diff_columns))

# COMMAND ----------

# Feature 21: Export Results
if export_option != 'None':
    export_path = f'/FileStore/recon_results.{export_option.lower()}'
    pandas_df = result_df.toPandas()
    if export_option == 'CSV':
        pandas_df.to_csv(f'/dbfs{export_path}', index=False)
    elif export_option == 'Excel':
        pandas_df.to_excel(f'/dbfs{export_path}', index=False)
    displayHTML(f"<a href='files/recon_results.{export_option.lower()}'>Download Reconciliation Results ({export_option})</a>")

# COMMAND ----------

# Feature 22: Save and Load Recon Configurations
# Simulating saving configurations by printing the current settings
configurations = {
    'attributes': attributes,
    'recon_option': recon_option,
    'numeric_threshold': numeric_threshold,
    'fuzzy_matching': fuzzy_matching,
    'export_option': export_option,
    'visualization_type': visualization_type
}
print("Current Reconciliation Configurations:")
print(configurations)

# COMMAND ----------

# Feature 23: Logging and Audit Trail
import datetime

log_entry = f"{datetime.datetime.now()}: Recon performed with options - Attributes: {attributes}, Recon Option: {recon_option}, Numeric Threshold: {numeric_threshold}, Fuzzy Matching: {fuzzy_matching}"
dbutils.fs.put('/FileStore/recon_logs.txt', log_entry + '\n', True)

# COMMAND ----------

# Feature 24: Performance Optimization (Using Cache)
source_df.cache()
target_df.cache()
joined_df.cache()
result_df.cache()

# COMMAND ----------

# Feature 25: User Guidance and Help Section
displayHTML("""
<h2>🛠 How to Use This Dashboard</h2>
<ul>
  <li><b>Select Datasets:</b> Choose the source and target datasets (using sample data).</li>
  <li><b>Custom Queries:</b> Optionally, provide PySpark SQL queries to filter or transform the datasets.</li>
  <li><b>Select Attributes:</b> Enter the columns you want to compare, separated by commas.</li>
  <li><b>Reconciliation Options:</b> Decide whether to view matches, mismatches, or all records.</li>
  <li><b>Numeric Threshold:</b> Set a threshold for numeric comparisons.</li>
  <li><b>Enable Fuzzy Matching:</b> Toggle fuzzy matching for string comparisons.</li>
  <li><b>Export Results:</b> Choose to export the results in CSV or Excel format.</li>
  <li><b>Visualization Type:</b> Select the type of chart to visualize the summary.</li>
</ul>
""")

# COMMAND ----------

# Feature 26: Integration with Alerting (Simulated Email Alerts)
alert_condition = result_df.filter(col('status') == '❌ Mismatch').count() > 0
if alert_condition:
    # Simulated email alert
    print("📧 Alert: Mismatches found in reconciliation!")

# COMMAND ----------

# Feature 27: Side-by-Side Comparison
def side_by_side(df, cols):
    left_cols = [f'src_{col}' for col in cols]
    right_cols = [f'tgt_{col}' for col in cols]
    select_expr = []
    for l_col, r_col in zip(left_cols, right_cols):
        select_expr.extend([col(l_col).alias(f'Source_{l_col.split("_", 1)[1]}'), col(r_col).alias(f'Target_{r_col.split("_", 1)[1]}')])
    return df.select('key', *select_expr, 'status')

side_by_side_df = side_by_side(result_df, attributes)
display(side_by_side_df)

# COMMAND ----------

# Feature 28: User Authentication and Access Control (Simulated)
user = dbutils.notebook.entry_point.getDbutils().notebook().getContext().tags().apply('user')
print(f"👤 Current User: {user}")

# COMMAND ----------

# Feature 29: Data Export to External Systems (Simulated)
# For demonstration, we'll just print a message
print("Data exported to external system (simulated).")

# COMMAND ----------

# Feature 30: Cleanup and Resource Release
source_df.unpersist()
target_df.unpersist()
joined_df.unpersist()
result_df.unpersist()
print("🧹 Resources cleaned up.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🎉 Thank You for Using the Advanced Data Reconciliation Dashboard!
# MAGIC
# MAGIC **Note:** This dashboard is designed with numerous features to provide a robust and user-friendly experience. Feel free to explore and customize it further to suit your specific needs.

