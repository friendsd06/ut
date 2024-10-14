# Load source data
source_df = spark.read.parquet("s3://your-s3-bucket/source_loan_data/")

# Generate 10 reconciliation scenarios by modifying the source data
scenarios = [
    # 1. Scenario 1: No difference
    source_df,

    # 2. Scenario 2: Change in `amount` for one loan
    source_df.withColumn("amount", when(col("loan_id") == 5, col("amount") + 500).otherwise(col("amount"))),

    # 3. Scenario 3: Change in `status` for some loans
    source_df.withColumn("status", when(col("loan_id").isin([10, 20]), "default").otherwise(col("status"))),

    # 4. Scenario 4: Change in `interest_rate` for specific loans
    source_df.withColumn("interest_rate", when(col("loan_id") == 15, col("interest_rate") + 2.0).otherwise(col("interest_rate"))),

    # 5. Scenario 5: Missing `loan_id` for a specific entry
    source_df.filter(col("loan_id") != 25),

    # 6. Scenario 6: Duplicate entry in the target dataset
    source_df.union(source_df.filter(col("loan_id") == 30)),

    # 7. Scenario 7: Extra column in the target dataset
    source_df.withColumn("extra_column", lit("extra_value")),

    # 8. Scenario 8: Different data type for `balance`
    source_df.withColumn("balance", col("balance").cast("string")),

    # 9. Scenario 9: Null values in `disbursed_date`
    source_df.withColumn("disbursed_date", when(col("loan_id") == 50, None).otherwise(col("disbursed_date"))),

    # 10. Scenario 10: Different format for `due_date`
    source_df.withColumn("due_date", expr("date_format(due_date, 'MM-dd-yyyy')"))
]

# Save each scenario as a separate target file in S3
for i, scenario_df in enumerate(scenarios, start=1):
    scenario_df.write.mode("overwrite").parquet(f"s3://your-s3-bucket/target_loan_data_scenario_{i}/")
