# Databricks notebook source
# MAGIC %md
# MAGIC # 🧮 Data Reconciliation Dashboard
# MAGIC
# MAGIC Welcome to the **Data Reconciliation Dashboard**! This interactive tool allows you to compare datasets between a source and a target, providing insights into matches and mismatches based on your selections.
# MAGIC
# MAGIC **Instructions:**
# MAGIC 1. **Select Source and Target Tables**: Use the dropdown menus below.
# MAGIC 2. **Optional Queries**: Enter custom PySpark SQL queries to filter or transform data.
# MAGIC 3. **Select Attributes**: Choose specific attributes (columns) for comparison.
# MAGIC 4. **Select Recon Option**: Choose to view matches, mismatches, or all records.
# MAGIC 5. **Run the Dashboard**: The results will display automatically based on your inputs.

# COMMAND ----------

# Clear existing widgets
dbutils.widgets.removeAll()

# COMMAND ----------

# Import necessary libraries
from pyspark.sql.functions import concat_ws, col, lit, when
import matplotlib.pyplot as plt

# COMMAND ----------

# Set up widgets

# Source table selection
source_tables = [table.name for table in spark.catalog.listTables('source_db')]
dbutils.widgets.dropdown('source_table', source_tables[0], source_tables, '🔹 Select Source Table')

# Target table selection
target_tables = [table.name for table in spark.catalog.listTables('target_db')]
dbutils.widgets.dropdown('target_table', target_tables[0], target_tables, '🔸 Select Target Table')

# PySpark SQL query input for source
dbutils.widgets.text('source_query', '', '📝 Source Query (Optional)')

# PySpark SQL query input for target
dbutils.widgets.text('target_query', '', '📝 Target Query (Optional)')

# Attribute selection (comma-separated)
dbutils.widgets.text('attributes', '', '🔎 Attributes (Comma-Separated)')

# Recon option selection
dbutils.widgets.dropdown('recon_option', 'match', ['match', 'mismatch', 'all'], '⚙️ Reconciliation Option')

# COMMAND ----------

# Get widget values
source_table = dbutils.widgets.get('source_table')
target_table = dbutils.widgets.get('target_table')
source_query = dbutils.widgets.get('source_query')
target_query = dbutils.widgets.get('target_query')
attributes = dbutils.widgets.get('attributes')
recon_option = dbutils.widgets.get('recon_option')

# COMMAND ----------

# Process attributes into a list
attribute_list = [attr.strip() for attr in attributes.split(',')] if attributes else None

# COMMAND ----------

# Load source DataFrame
if source_query.strip():
    source_df = spark.sql(source_query)
else:
    source_df = spark.table(f'source_db.{source_table}')

# Load target DataFrame
if target_query.strip():
    target_df = spark.sql(target_query)
else:
    target_df = spark.table(f'target_db.{target_table}')

# COMMAND ----------

# Select specific attributes if provided
if attribute_list:
    source_df = source_df.select(attribute_list)
    target_df = target_df.select(attribute_list)

# COMMAND ----------

# Create a unique key based on the attributes
if attribute_list:
    key_cols = attribute_list
else:
    key_cols = source_df.columns

# Add a key column to both DataFrames
source_df = source_df.withColumn('key', concat_ws('_', *key_cols))
target_df = target_df.withColumn('key', concat_ws('_', *key_cols))

# COMMAND ----------

# Perform full outer join on key
joined_df = source_df.alias('src').join(
    target_df.alias('tgt'),
    on='key',
    how='full_outer'
)

# COMMAND ----------

# Identify matches and mismatches
joined_df = joined_df.withColumn('status', when(
    col('src.key').isNotNull() & col('tgt.key').isNotNull(), '✅ match'
).when(
    col('src.key').isNotNull() & col('tgt.key').isNull(), '❌ mismatch_source_only'
).when(
    col('src.key').isNull() & col('tgt.key').isNotNull(), '❌ mismatch_target_only'
).otherwise('unknown'))

# COMMAND ----------

# Filter results based on recon option
if recon_option == 'match':
    result_df = joined_df.filter(col('status') == '✅ match')
elif recon_option == 'mismatch':
    result_df = joined_df.filter(col('status') != '✅ match')
elif recon_option == 'all':
    result_df = joined_df
else:
    result_df = joined_df

# COMMAND ----------

# Display the result
display(result_df.select(*key_cols, 'status'))

# COMMAND ----------

# Compute summary statistics
total_source = source_df.count()
total_target = target_df.count()
total_matches = joined_df.filter(col('status') == '✅ match').count()
total_mismatches = joined_df.filter(col('status') != '✅ match').count()

# COMMAND ----------

# Create summary DataFrame
summary_data = [
    ('Total Records in Source', total_source),
    ('Total Records in Target', total_target),
    ('Total Matches', total_matches),
    ('Total Mismatches', total_mismatches)
]

summary_df = spark.createDataFrame(summary_data, ['Metric', 'Value'])

# Display summary table
display(summary_df)

# COMMAND ----------

# Convert summary DataFrame to Pandas for plotting
summary_pd = summary_df.toPandas()

# Plotting
plt.figure(figsize=(10,6))
plt.bar(summary_pd['Metric'], summary_pd['Value'], color=['#1f77b4', '#ff7f0e', '#2ca02c', '#d62728'])
plt.title('🔍 Reconciliation Summary', fontsize=16)
plt.xlabel('Metric', fontsize=12)
plt.ylabel('Value', fontsize=12)
plt.xticks(rotation=45, fontsize=10)
plt.yticks(fontsize=10)
plt.grid(axis='y', linestyle='--', alpha=0.7)
plt.tight_layout()
display(plt.gcf())

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📊 Additional Insights
# MAGIC
# MAGIC - **Match Percentage**: `{(total_matches / max(total_source, total_target)) * 100:.2f}%`
# MAGIC - **Mismatch Percentage**: `{(total_mismatches / max(total_source, total_target)) * 100:.2f}%`
# MAGIC
# MAGIC **Interpretation:**
# MAGIC - A higher match percentage indicates greater consistency between the source and target datasets.
# MAGIC - Analyzing mismatches can help identify data discrepancies or integration issues.

# COMMAND ----------

# Export results as CSV (optional)
# Uncomment the following lines to save the result to a CSV file in DBFS

# output_path = '/FileStore/recon_results.csv'
# result_df.select(*key_cols, 'status').coalesce(1).write.mode('overwrite').option('header', 'true').csv(output_path)
# displayHTML(f"<a href='files/recon_results.csv'>Download Reconciliation Results</a>")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🚀 Enhance Your Analysis
# MAGIC
# MAGIC - **Drill-Down Analysis**: Filter the mismatches to identify specific records causing discrepancies.
# MAGIC - **Custom Visualizations**: Integrate with advanced visualization libraries like Plotly for interactive charts.
# MAGIC - **Automate Reporting**: Schedule this notebook to run at regular intervals and send email notifications.

# COMMAND ----------

# Clean up temporary views if created
# spark.catalog.dropTempView('source_temp_view')
# spark.catalog.dropTempView('target_temp_view')
