# Databricks notebook source
# MAGIC %md
# MAGIC # 🚀 Advanced Data Reconciliation Dashboard
# MAGIC
# MAGIC Welcome to the **Advanced Data Reconciliation Dashboard**! This dashboard provides a comprehensive and user-friendly interface to perform data reconciliation between two datasets. It includes several features to help you identify and analyze differences between your source and target data.

# COMMAND ----------

# Import necessary libraries
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    concat_ws, col, lit, when, abs, isnan, count, sum as F_sum
)
from pyspark.sql.types import *
import matplotlib.pyplot as plt
import pandas as pd

# COMMAND ----------

# Clear existing widgets
dbutils.widgets.removeAll()

# COMMAND ----------

# Create sample data for source and target datasets

# Sample data for Source
source_data = [
    (1, 'Alice', 28, 'New York', 70000.0),
    (2, 'Bob', 35, 'Los Angeles', 80000.0),
    (3, 'Charlie', 25, 'Chicago', 65000.0),
    (4, 'Diana', 30, 'Houston', 72000.0),
    (5, 'Ethan', 45, 'Phoenix', 90000.0)
]

# Sample data for Target
target_data = [
    (1, 'Alice', 28, 'New York', 70000.0),
    (2, 'Bob', 36, 'Los Angeles', 80000.0),  # Age mismatch
    (3, 'Charlie', None, 'Chicago', 65000.0),  # Missing Age
    (4, 'Diana', 30, 'Houston', 75000.0),  # Salary mismatch
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

# MAGIC %md
# MAGIC ## 📋 User Inputs

# COMMAND ----------

# Feature 1: Widget for selecting key columns
dbutils.widgets.text('key_columns', 'ID', '🔑 Key Columns (Comma-separated)')

# Feature 2: Widget for selecting columns to compare
dbutils.widgets.text('compare_columns', 'Name,Age,City,Salary', '📝 Columns to Compare (Comma-separated)')

# Feature 3: Option to ignore columns
dbutils.widgets.text('ignore_columns', '', '🚫 Columns to Ignore (Comma-separated)')

# Feature 4: Numeric tolerance for comparisons
dbutils.widgets.text('numeric_tolerance', '0', '📐 Numeric Tolerance')

# Feature 5: Option to handle null values
dbutils.widgets.dropdown('null_handling', 'Exclude Nulls', ['Exclude Nulls', 'Include Nulls'], '🔍 Null Handling')

# Feature 6: Option to specify join type
dbutils.widgets.dropdown('join_type', 'Outer', ['Inner', 'Left', 'Right', 'Outer'], '🔗 Join Type')

# Feature 7: Option to select match types to display
dbutils.widgets.text('match_types', 'Matched,Mismatched,Source Only,Target Only', '🎯 Match Types to Display (Comma-separated)')

# Feature 8: Option to save reconciliation report
dbutils.widgets.dropdown('save_report', 'No', ['Yes', 'No'], '💾 Save Reconciliation Report')

# Feature 9: Report name input
dbutils.widgets.text('report_name', 'recon_report', '📝 Report Name (without extension)')

# Feature 10: Option to display summary statistics
dbutils.widgets.dropdown('show_summary', 'Yes', ['Yes', 'No'], '📊 Show Summary Statistics')

# Feature 11: Option to display detailed mismatches
dbutils.widgets.dropdown('show_detailed_mismatches', 'Yes', ['Yes', 'No'], '🔎 Show Detailed Mismatches')

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🔄 Data Preparation

# COMMAND ----------

# Retrieve widget values
key_columns = [col.strip() for col in dbutils.widgets.get('key_columns').split(',') if col.strip()]
compare_columns = [col.strip() for col in dbutils.widgets.get('compare_columns').split(',') if col.strip()]
ignore_columns = [col.strip() for col in dbutils.widgets.get('ignore_columns').split(',') if col.strip()]
numeric_tolerance = float(dbutils.widgets.get('numeric_tolerance'))
null_handling = dbutils.widgets.get('null_handling')
join_type = dbutils.widgets.get('join_type').lower()
match_types = [mt.strip() for mt in dbutils.widgets.get('match_types').split(',') if mt.strip()]
save_report = dbutils.widgets.get('save_report')
report_name = dbutils.widgets.get('report_name')
show_summary = dbutils.widgets.get('show_summary') == 'Yes'
show_detailed_mismatches = dbutils.widgets.get('show_detailed_mismatches') == 'Yes'

# COMMAND ----------

# Apply ignore columns
compare_columns = [col for col in compare_columns if col not in ignore_columns]

# Check if key columns and compare columns are in the dataframes
all_columns = set(source_df.columns) & set(target_df.columns)
for col in key_columns + compare_columns:
    if col not in all_columns:
        raise ValueError(f"Column '{col}' not found in both source and target datasets.")

# COMMAND ----------

# Feature 12: Handle null values
if null_handling == 'Exclude Nulls':
    source_df = source_df.dropna(subset=key_columns + compare_columns)
    target_df = target_df.dropna(subset=key_columns + compare_columns)

# COMMAND ----------

# Feature 13: Add a unique key column for joining
source_df = source_df.withColumn('join_key', concat_ws('_', *key_columns))
target_df = target_df.withColumn('join_key', concat_ws('_', *key_columns))

# COMMAND ----------

# Feature 14: Perform the join based on user-selected join type
joined_df = source_df.alias('src').join(
    target_df.alias('tgt'),
    on='join_key',
    how=join_type
)

# COMMAND ----------

# Feature 15: Compare columns and identify matches/mismatches
from pyspark.sql.functions import when

comparison_expressions = []
for col_name in compare_columns:
    src_col = col(f'src.{col_name}')
    tgt_col = col(f'tgt.{col_name}')
    data_type = [field.dataType for field in schema if field.name == col_name][0]
    if isinstance(data_type, (IntegerType, DoubleType, FloatType, LongType, ShortType)):
        comparison = when(abs(src_col - tgt_col) <= numeric_tolerance, True).otherwise(False)
    else:
        comparison = when(src_col == tgt_col, True).otherwise(False)
    comparison_expressions.append(comparison.alias(f'{col_name}_match'))

joined_df = joined_df.select(
    'join_key',
    *[col(f'src.{col}').alias(f'src_{col}') for col in compare_columns],
    *[col(f'tgt.{col}').alias(f'tgt_{col}') for col in compare_columns],
    *comparison_expressions,
    col('src.ID').alias('src_ID'),
    col('tgt.ID').alias('tgt_ID')
)

# COMMAND ----------

# Feature 16: Determine match status
def get_match_status(src_id, tgt_id, *matches):
    if src_id is not None and tgt_id is not None:
        if all(matches):
            return 'Matched'
        else:
            return 'Mismatched'
    elif src_id is not None:
        return 'Source Only'
    else:
        return 'Target Only'

from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

match_status_udf = udf(get_match_status, StringType())
joined_df = joined_df.withColumn('Match_Status', match_status_udf(
    col('src_ID'), col('tgt_ID'), *[col(f'{col}_match') for col in compare_columns]
))

# COMMAND ----------

# Feature 17: Filter results based on user-selected match types
joined_df = joined_df.filter(col('Match_Status').isin(match_types))

# COMMAND ----------

# Feature 18: Display summary statistics
if show_summary:
    total_records = joined_df.count()
    match_counts = joined_df.groupBy('Match_Status').count().toPandas()
    displayHTML("<h3>📊 Reconciliation Summary</h3>")
    display(match_counts)

    # Feature 19: Visualize summary statistics
    match_counts.plot(kind='bar', x='Match_Status', y='count', legend=False, title='Reconciliation Summary')
    plt.ylabel('Record Count')
    plt.show()

# COMMAND ----------

# Feature 20: Display detailed mismatches
if show_detailed_mismatches:
    displayHTML("<h3>🔎 Detailed Mismatches</h3>")
    mismatches_df = joined_df.filter(col('Match_Status') == 'Mismatched')
    if mismatches_df.count() > 0:
        display(mismatches_df)
    else:
        displayHTML("<p>No mismatches found based on the selected criteria.</p>")

# COMMAND ----------

# Feature 21: Export reconciliation report
if save_report == 'Yes':
    output_path = f'/FileStore/{report_name}.csv'
    joined_df.toPandas().to_csv(f'/dbfs{output_path}', index=False)
    displayHTML(f"<a href='files/{report_name}.csv'>Download Reconciliation Report</a>")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🛠️ Additional Features

# COMMAND ----------

# Feature 22: Column-wise mismatch counts
column_mismatch_counts = {}
for col_name in compare_columns:
    mismatch_count = joined_df.filter(col(f'{col_name}_match') == False).count()
    column_mismatch_counts[col_name] = mismatch_count

column_mismatch_df = pd.DataFrame(list(column_mismatch_counts.items()), columns=['Column', 'Mismatch_Count'])
displayHTML("<h3>📄 Column-wise Mismatch Counts</h3>")
display(column_mismatch_df)

# COMMAND ----------

# Feature 23: Percentage of mismatches per column
total_matched_records = joined_df.filter(col('Match_Status') == 'Matched').count()
column_mismatch_df['Mismatch_Percentage'] = (column_mismatch_df['Mismatch_Count'] / total_records) * 100
display(column_mismatch_df)

# COMMAND ----------

# Feature 24: Side-by-side comparison of mismatched records
if show_detailed_mismatches and mismatches_df.count() > 0:
    displayHTML("<h3>📝 Side-by-Side Comparison of Mismatched Records</h3>")
    mismatch_columns = [f'src_{col}' for col in compare_columns] + [f'tgt_{col}' for col in compare_columns]
    display(mismatches_df.select('join_key', *mismatch_columns))

# COMMAND ----------

# Feature 25: Option to specify output format (CSV or Excel)
dbutils.widgets.dropdown('output_format', 'CSV', ['CSV', 'Excel'], '📁 Output Format')
output_format = dbutils.widgets.get('output_format')
if save_report == 'Yes':
    if output_format == 'Excel':
        output_path = f'/FileStore/{report_name}.xlsx'
        joined_df.toPandas().to_excel(f'/dbfs{output_path}', index=False)
        displayHTML(f"<a href='files/{report_name}.xlsx'>Download Reconciliation Report</a>")
    else:
        # Already saved as CSV above
        pass

# COMMAND ----------

# Feature 26: Option to include unmatched records
dbutils.widgets.dropdown('include_unmatched', 'Yes', ['Yes', 'No'], '📋 Include Unmatched Records')
include_unmatched = dbutils.widgets.get('include_unmatched') == 'Yes'

if include_unmatched:
    unmatched_records = joined_df.filter(col('Match_Status').isin(['Source Only', 'Target Only']))
    if unmatched_records.count() > 0:
        displayHTML("<h3>🔍 Unmatched Records</h3>")
        display(unmatched_records)
    else:
        displayHTML("<p>No unmatched records found based on the selected criteria.</p>")

# COMMAND ----------

# Feature 27: Data profiling of source and target datasets
def data_profiling(df, df_name):
    total_records = df.count()
    missing_counts = df.select([F_sum(col(c).isNull().cast("int")).alias(c) for c in df.columns]).collect()[0].asDict()
    missing_counts = {k: v for k, v in missing_counts.items() if v > 0}
    profile = {'Total Records': total_records, 'Missing Values': missing_counts}
    displayHTML(f"<h3>📈 Data Profiling for {df_name}</h3>")
    display(pd.DataFrame([profile]))

data_profiling(source_df, 'Source Dataset')
data_profiling(target_df, 'Target Dataset')

# COMMAND ----------

# Feature 28: Clean up temporary columns
joined_df = joined_df.drop('join_key', 'src_ID', 'tgt_ID')
source_df = source_df.drop('join_key')
target_df = target_df.drop('join_key')

# COMMAND ----------

# Feature 29: Option to adjust display settings
pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', 100)

# COMMAND ----------

# Feature 30: Final reconciliation summary
displayHTML("<h2>✅ Reconciliation Completed</h2>")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🎉 Thank You for Using the Advanced Data Reconciliation Dashboard!

