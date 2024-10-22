# ... [Import statements and initial code remain unchanged]

# Sample data for source DataFrame
source_data = [
    {
        "parent_primary_key": "P001",
        "child_primary_key": "C001",
        "name": "John Doe",
        "age": 30,
        "address": {"street": "123 Main St", "city": "CityA", "zipcode": "12345"},
        "orders": [
            {"order_id": "O001", "order_date": "2021-01-01", "amount": 150.0, "status": "shipped"},
            {"order_id": "O002", "order_date": "2021-02-01", "amount": 200.0, "status": "pending"}
        ],
        "payments": [
            {"payment_id": "P001", "payment_date": "2021-01-02", "method": "credit_card", "amount": 150.0},
            {"payment_id": "P002", "payment_date": "2021-02-02", "method": "paypal", "amount": 70.0}
        ],
        "new_array": [
            {"new_id1": "N001", "new_id2": "N002", "detail": "DetailA", "extra_info": "InfoA"}
        ]
    }
]

# Sample data for target DataFrame with some differences
target_data = [
    {
        "parent_primary_key": "P001",
        "child_primary_key": "C001",
        "name": "John Doe",
        "age": 31,  # Age difference
        "address": {"street": "123 Main St", "city": "CityA", "zipcode": "12345"},
        "orders": [
            {"order_id": "O001", "order_date": "2021-01-01", "amount": 200.0, "status": "shipped"},  # Amount difference
            {"order_id": "O002", "order_date": "2021-02-01", "amount": 200.0, "status": "completed"}  # Status difference
        ],
        "payments": [
            {"payment_id": "P001", "payment_date": "2021-01-02", "method": "credit_card", "amount": 150.0},
            {"payment_id": "P002", "payment_date": "2021-02-02", "method": "paypal", "amount": 80.0}  # Amount difference
        ],
        "new_array": [
            {"new_id1": "N001", "new_id2": "N002", "detail": "DetailB", "extra_info": "InfoA"}  # Detail difference
        ]
    }
]

# Create DataFrames with the defined schemas
source_df = spark.createDataFrame(source_data, main_schema)
target_df = spark.createDataFrame(target_data, main_schema)

# ... [Function definitions remain unchanged until compare_and_combine_differences]

def compare_and_combine_differences(
        joined_df, compare_fields, source_prefix, target_prefix,
        global_primary_keys, array_pks, result_col_name
):
# ... [Function body as shown above]

# Define global and array-specific primary keys
global_primary_keys = ["parent_primary_key", "child_primary_key"]
array_columns = {
    "orders": ["order_id"],
    "payments": ["payment_id"],
    "new_array": ["new_id1", "new_id2"]
}

# Explode arrays and prefix columns
source_exploded_dfs = explode_and_prefix(source_df, array_columns, "source_", global_primary_keys)
target_exploded_dfs = explode_and_prefix(target_df, array_columns, "", global_primary_keys)

# Join exploded DataFrames
joined_dfs = join_exploded_dfs(source_exploded_dfs, target_exploded_dfs, array_columns, global_primary_keys)

# Define fields to compare for each array column
fields_to_compare = {
    "orders": [],        # Empty list means compare all fields
    "payments": None,    # None means compare all fields
    "new_array": ["detail"]  # Compare only 'detail' field
}

# Compare fields and collect differences
difference_results = {}
for array_col, array_pks in array_columns.items():
    joined_df = joined_dfs[array_col]
    compare_fields = fields_to_compare.get(array_col)
    result_col_name = f"{array_col}_differences"
    diff_df = compare_and_combine_differences(
        joined_df, compare_fields, "source_", "target_", global_primary_keys, array_pks, result_col_name
    )
    difference_results[array_col] = diff_df

# Display the differences
def display_differences(difference_results):
    for array_col, diff_df in difference_results.items():
        print(f"=== Differences in {array_col.capitalize()} ===")
        diff_df.show(truncate=False)
        print("\n")

display_differences(difference_results)

# Stop the SparkSession
spark.stop()
