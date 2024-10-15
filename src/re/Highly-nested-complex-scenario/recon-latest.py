# Import necessary libraries
from pyspark.sql import SparkSession
from datetime import datetime
import json

# Function to create or reset widgets
def create_widgets():
    # Clear existing widgets to avoid duplication
    dbutils.widgets.removeAll()

    # COB-DATE Widget (Date Picker)
    dbutils.widgets.text("COB_DATE", datetime.today().strftime('%Y-%m-%d'), "COB Date (YYYY-MM-DD)")

    # S3 Path Dropdown
    s3_paths = [
        "s3://bucket/path1/",
        "s3://bucket/path2/",
        "s3://bucket/path3/"
    ]
    dbutils.widgets.dropdown("S3_PATH", s3_paths[0], s3_paths, "Select S3 Path")

    # Environment Dropdown
    environments = ["Development", "Staging", "Production"]
    dbutils.widgets.dropdown("ENVIRONMENT", environments[0], environments, "Select Environment")

    # Delta Table Dropdown
    delta_tables = ["table1", "table2", "table3"]
    dbutils.widgets.dropdown("DELTA_TABLE", delta_tables[0], delta_tables, "Select Delta Table")

    # Hidden Widget to act as a button trigger
    dbutils.widgets.text("RUN_BUTTON", "No", "Run Reconciliation")

# Function to render the UI
def render_ui():
    # Fetch current widget values
    cob_date = dbutils.widgets.get("COB_DATE")
    s3_path = dbutils.widgets.get("S3_PATH")
    environment = dbutils.widgets.get("ENVIRONMENT")
    delta_table = dbutils.widgets.get("DELTA_TABLE")

    ui_html = f"""
    <style>
        .container {{
            display: flex;
            flex-direction: column;
            align-items: center;
            justify-content: center;
            padding: 20px;
            font-family: Arial, sans-serif;
        }}
        .input-group {{
            margin: 10px 0;
            width: 300px;
            display: flex;
            justify-content: space-between;
            align-items: center;
        }}
        .input-group label {{
            margin-right: 10px;
            font-weight: bold;
        }}
        .run-button {{
            padding: 10px 20px;
            background-color: #1f77b4;
            color: white;
            border: none;
            border-radius: 5px;
            cursor: pointer;
            font-size: 16px;
            margin-top: 20px;
        }}
        .run-button:hover {{
            background-color: #155a8a;
        }}
    </style>
    
    <div class="container">
        <h2>Data Reconciliation Interface</h2>
        
        <div class="input-group">
            <label for="COB_DATE">COB Date:</label>
            <input type="date" id="COB_DATE" name="COB_DATE" value="{cob_date}" onchange="updateWidget('COB_DATE', this.value)">
        </div>
        
        <div class="input-group">
            <label for="S3_PATH">S3 Path:</label>
            <select id="S3_PATH" name="S3_PATH" onchange="updateWidget('S3_PATH', this.value)">
                {''.join([f'<option value="{path}" {"selected" if path == s3_path else ""}>{path}</option>' for path in ["s3://bucket/path1/", "s3://bucket/path2/", "s3://bucket/path3/"]])}
            </select>
        </div>
        
        <div class="input-group">
            <label for="ENVIRONMENT">Environment:</label>
            <select id="ENVIRONMENT" name="ENVIRONMENT" onchange="updateWidget('ENVIRONMENT', this.value)">
                {''.join([f'<option value="{env}" {"selected" if env == environment else ""}>{env}</option>' for env in ["Development", "Staging", "Production"]])}
            </select>
        </div>
        
        <div class="input-group">
            <label for="DELTA_TABLE">Delta Table:</label>
            <select id="DELTA_TABLE" name="DELTA_TABLE" onchange="updateWidget('DELTA_TABLE', this.value)">
                {''.join([f'<option value="{table}" {"selected" if table == delta_table else ""}>{table}</option>' for table in ["table1", "table2", "table3"]])}
            </select>
        </div>
        
        <button class="run-button" onclick="runReconciliation()">Run Reconciliation</button>
    </div>
    
    <script>
        // Function to update widget values
        function updateWidget(widgetName, value) {{
            // Databricks doesn't allow direct manipulation of widgets via JavaScript.
            // So, we'll use a workaround by setting the RUN_BUTTON value to 'Yes'.
            // The notebook will detect this change and reset RUN_BUTTON to 'No'.
            // Then, it can perform actions based on widget values.
        }}
        
        // Function to trigger reconciliation
        function runReconciliation() {{
            // Trigger the RUN_BUTTON by setting its value to 'Yes'
            // Since JavaScript cannot directly set Databricks widgets, we use a hidden cell.
            // We'll leverage a hidden iframe to communicate with the notebook.
            var iframe = document.createElement('iframe');
            iframe.style.display = 'none';
            iframe.src = "/?run_button=Yes";
            document.body.appendChild(iframe);
        }}
    </script>
    """
    displayHTML(ui_html)

# Function to perform reconciliation (placeholder implementation)
def run_reconciliation(cob_date, s3_path, environment, delta_table):
    """
    Perform reconciliation between two Delta tables based on provided parameters.

    :param cob_date: COB Date as a string.
    :param s3_path: Selected S3 path.
    :param environment: Selected environment.
    :param delta_table: Selected Delta table.
    """
    # Placeholder for reconciliation logic
    # Replace this with actual implementation
    print(f"Running reconciliation for COB Date: {cob_date}")
    print(f"S3 Path: {s3_path}")
    print(f"Environment: {environment}")
    print(f"Delta Table: {delta_table}")

    # Example: Simulate reconciliation process
    import time
    time.sleep(2)  # Simulate processing time
    print("Reconciliation Completed Successfully!")

    # Example: Display a success message
    success_html = f"""
    <div style='margin-top:20px; padding:10px; border:2px solid green; border-radius:5px; background-color:#d4edda; color:#155724;'>
        <h3>Reconciliation Completed Successfully!</h3>
        <p>COB Date: {cob_date}</p>
        <p>S3 Path: {s3_path}</p>
        <p>Environment: {environment}</p>
        <p>Delta Table: {delta_table}</p>
    </div>
    """
    displayHTML(success_html)

# Function to check if reconciliation should run
def check_and_run_reconciliation():
    run_button = dbutils.widgets.get("RUN_BUTTON")
    if run_button.lower() == "yes":
        # Get widget values
        cob_date = dbutils.widgets.get("COB_DATE")
        s3_path = dbutils.widgets.get("S3_PATH")
        environment = dbutils.widgets.get("ENVIRONMENT")
        delta_table = dbutils.widgets.get("DELTA_TABLE")

        # Run reconciliation
        run_reconciliation(cob_date, s3_path, environment, delta_table)

        # Reset RUN_BUTTON to 'No' to prevent repeated runs
        dbutils.widgets.remove("RUN_BUTTON")
        dbutils.widgets.text("RUN_BUTTON", "No", "Run Reconciliation")

# Function to save ignore list
def save_ignore_list(ignore_list: set, file_path: str = "ignore_list.txt") -> None:
    """
    Save the ignore list to a text file.

    :param ignore_list: Set of column names to ignore.
    :param file_path: Destination file path.
    """
    with open(file_path, 'w') as f:
        for attr in sorted(ignore_list):
            f.write(f"{attr}\n")
    print(f"Ignore list saved to {file_path}.")

# Function to test delta table comparison (placeholder)
def test_delta_table_comparison(spark: SparkSession) -> None:
    """
    Test the Delta table comparison with a mixed schema scenario.

    :param spark: Active SparkSession.
    """
    print("Testing Delta Table Comparison with Mixed Schema Scenario...")
    # Placeholder for actual test implementation

# Function to render hidden RUN_BUTTON input (for JavaScript workaround)
def render_hidden_run_button():
    hidden_html = f"""
    <input type="hidden" id="RUN_BUTTON" value="{dbutils.widgets.get('RUN_BUTTON')}" />
    """
    displayHTML(hidden_html)

# Assemble the notebook
def main():
    """
    Main function to execute the Delta table comparison test.
    """
    # Initialize Widgets
    create_widgets()

    # Render the User Interface
    render_ui()

    # Render hidden run button
    render_hidden_run_button()

    # Check and run reconciliation if button was clicked
    check_and_run_reconciliation()

# Execute the main function
main()
