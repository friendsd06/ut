# Example: Load and reconcile Scenario 2
source_df = spark.read.parquet("s3://your-s3-bucket/source_loan_data/")
target_df = spark.read.parquet("s3://your-s3-bucket/target_loan_data_scenario_2/")

# Define reconciliation function (simplified version)
def reconcile_dataframes(df1, df2, join_key="loan_id"):
    comparison_df = df1.alias("df1").join(df2.alias("df2"), join_key, "outer")
    mismatched_columns = [
        col(f"df1.{column}").alias(f"source_{column}"),
        col(f"df2.{column}").alias(f"target_{column}")
    for column in df1.columns if column in df2.columns and column != join_key
    ]
    comparison_df.select(*[col(join_key)] + mismatched_columns).show(truncate=False)

# Reconcile Scenario 2
reconcile_dataframes(source_df, target_df)


Summary of Scenarios:
Scenario 1: Identical data, no discrepancies.
Scenario 2: Amount discrepancy for a specific loan.
Scenario 3: Status change for selected loans.
    Scenario 4: Interest rate difference for certain loans.
    Scenario 5: Missing loan entry in the target.
Scenario 6: Duplicate loan entry in the target.
Scenario 7: Additional column in the target.
Scenario 8: Data type change in balance.
Scenario 9: Null value in the disbursed_date.
Scenario 10: Date format change in due_date.