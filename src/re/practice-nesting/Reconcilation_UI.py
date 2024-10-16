# UI_Reconciliation Notebook

import ipywidgets as widgets
from IPython.display import display, HTML

# Include the backend notebook
# Ensure that the path is correct relative to this notebook
%run "/Path/To/Backend_Reconciliation"

# Create widgets for user input
environment_widget = widgets.Dropdown(
    options=['Development', 'Testing', 'Production'],
    value='Production',
    description='Environment:',
    disabled=False,
)

dataset_widget = widgets.Dropdown(
    options=['Dataset1', 'Dataset2', 'Dataset3'],  # Update with actual dataset names
    value='Dataset1',
    description='Dataset:',
    disabled=False,
)

cobdate_widget = widgets.DatePicker(
    description='COB Date:',
    disabled=False
)

columns_widget = widgets.SelectMultiple(
    options=[],  # To be populated based on selected dataset
    description='Columns to Reconcile:',
    disabled=False
)

run_button = widgets.Button(
    description="Run Reconciliation",
    button_style='success'
)

output = widgets.Output()

# Function to update columns based on selected dataset
def update_columns(*args):
    selected_dataset = dataset_widget.value
    # Logic to fetch columns based on the selected dataset
    # For demonstration, using placeholder columns
    if selected_dataset == 'Dataset1':
        common_columns = ['id', 'name', 'age', 'salary']
    elif selected_dataset == 'Dataset2':
        common_columns = ['id', 'product', 'quantity', 'price']
    else:
        common_columns = ['id', 'category', 'value', 'date']
    columns_widget.options = common_columns
    columns_widget.value = tuple(common_columns)  # Select all by default

dataset_widget.observe(update_columns, names='value')

# Initial population of columns
update_columns()

# Run Reconciliation Button Click Handler
def run_reconciliation(b):
    with output:
        output.clear_output()
        print("Running reconciliation...")
        # Trigger the reconciliation process
        try:
            perform_reconciliation(
                environment=environment_widget.value,
                dataset=dataset_widget.value,
                cobdate=cobdate_widget.value,
                columns_to_check=list(columns_widget.value)
            )
            print("\nReconciliation completed successfully!")
        except Exception as e:
            print(f"An error occurred during reconciliation: {e}")

run_button.on_click(run_reconciliation)

# Display the UI
ui = widgets.VBox([
    environment_widget,
    dataset_widget,
    cobdate_widget,
    columns_widget,
    run_button,
    output
])

display(ui)