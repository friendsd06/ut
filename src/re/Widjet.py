# Clear existing widgets
dbutils.widgets.removeAll()

# Display instructions
displayHTML("""
<h2>Data Reconciliation Dashboard</h2>
<p>Please provide the following parameters to compare two tables:</p>
<ul>
  <li><strong>First Table Name:</strong> Name of the first Delta table.</li>
  <li><strong>Second Table Name:</strong> Name of the second Delta table.</li>
  <li><strong>Join Key:</strong> The column used to join the two tables.</li>
  <li><strong>Include Columns:</strong> (Optional) Comma-separated list of columns to include in the comparison.</li>
  <li><strong>Exclude Columns:</strong> (Optional) Comma-separated list of columns to exclude from the comparison.</li>
  <li><strong>Start Date:</strong> (Optional) Start date for filtering data (YYYY-MM-DD).</li>
  <li><strong>End Date:</strong> (Optional) End date for filtering data (YYYY-MM-DD).</li>
</ul>
""")

# Define Widgets
dbutils.widgets.text("table1", "table1_name", "First Table Name")
dbutils.widgets.text("table2", "table2_name", "Second Table Name")
dbutils.widgets.text("join_key", "id", "Join Key")
dbutils.widgets.text("include_columns", "", "Include Columns (comma-separated)")
dbutils.widgets.text("exclude_columns", "", "Exclude Columns (comma-separated)")
dbutils.widgets.text("start_date", "2024-01-01", "Start Date (YYYY-MM-DD)")
dbutils.widgets.text("end_date", "2024-12-31", "End Date (YYYY-MM-DD)")


# Retrieve widget values
table1_name = dbutils.widgets.get("table1")
table2_name = dbutils.widgets.get("table2")
join_key = dbutils.widgets.get("join_key")
include_columns_input = dbutils.widgets.get("include_columns")
exclude_columns_input = dbutils.widgets.get("exclude_columns")
start_date = dbutils.widgets.get("start_date")
end_date = dbutils.widgets.get("end_date")

# Process comma-separated columns into lists
include_columns = [col.strip() for col in include_columns_input.split(",") if col.strip()] if include_columns_input else None
exclude_columns = [col.strip() for col in exclude_columns_input.split(",") if col.strip()] if exclude_columns_input else None

# Convert date strings to date objects if needed
from datetime import datetime

try:
    start_date_obj = datetime.strptime(start_date, "%Y-%m-%d").date()
    end_date_obj = datetime.strptime(end_date, "%Y-%m-%d").date()
except ValueError:
    raise ValueError("Start Date and End Date must be in YYYY-MM-DD format.")
