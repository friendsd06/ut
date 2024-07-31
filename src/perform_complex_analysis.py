# COMMAND ----------
from pyspark.sql.functions import col, sum, avg, max, min, count, datediff, year, month, when, rank, current_date
from pyspark.sql.window import Window
import time

# Disable AQE and skew join optimization
spark.conf.set("spark.sql.adaptive.enabled", "false")
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "false")

# Verify configurations
print("AQE enabled:", spark.conf.get("spark.sql.adaptive.enabled"))
print("Skew join optimization enabled:", spark.conf.get("spark.sql.adaptive.skewJoin.enabled"))


# Read the data
customer_df = spark.read.csv(customer_path, header=True, schema=customer_schema)
loan_df = spark.read.csv(loan_path, header=True, schema=loan_schema)

# Complex operation
def perform_complex_analysis():
    # 1. Join loan and customer data
    joined_df = loan_df.join(customer_df, "customer_id")

    # 2. Calculate loan statistics per customer
    customer_loan_stats = joined_df.groupBy("customer_id").agg(
        count("loan_id").alias("loan_count"),
        sum("loan_amount").alias("total_loan_amount"),
        avg("interest_rate").alias("avg_interest_rate"),
        sum(when(col("loan_status") == "Defaulted", 1).otherwise(0)).alias("defaulted_loans")
    )

    # 3. Calculate risk score (this will cause skew due to the window function)
    window_spec = Window.orderBy(col("total_loan_amount").desc())
    risk_scores = customer_loan_stats.withColumn(
        "risk_score",
        rank().over(window_spec) +
        (col("defaulted_loans") / col("loan_count") * 100) +
        (col("total_loan_amount") / 10000)
    )

    # 4. Join back to the original data
    enriched_df = joined_df.join(risk_scores, "customer_id")

    # 5. Complex aggregation and filtering
    result = enriched_df.filter(col("loan_status") != "Paid Off") \
        .groupBy("customer_id", "state", "loan_type", year("origination_date").alias("origination_year")) \
        .agg(
        count("loan_id").alias("active_loans"),
        sum("remaining_balance").alias("total_outstanding_balance"),
        avg("risk_score").alias("avg_risk_score"),
        max("credit_score_at_origination").alias("max_credit_score"),
        min("debt_to_income_ratio").alias("min_dti")
    ) \
        .filter(col("total_outstanding_balance") > 100000) \
        .orderBy(col("avg_risk_score").desc())

    # 6. Additional join with a derived table (this will further increase complexity and potential skew)
    state_stats = result.groupBy("state").agg(
        avg("avg_risk_score").alias("state_avg_risk_score"),
        sum("total_outstanding_balance").alias("state_total_balance")
    )

    final_result = result.join(state_stats, "state") \
        .withColumn("risk_score_vs_state_avg", col("avg_risk_score") - col("state_avg_risk_score")) \
        .orderBy(col("risk_score_vs_state_avg").desc())

    return final_result

# Execute the complex operation
print("Performing complex analysis...")
start_time = time.time()
result_df = perform_complex_analysis()
end_time = time.time()

print(f"Analysis completed in {end_time - start_time:.2f} seconds")
print("Sample result:")
result_df.show(10, truncate=False)

# Show execution plan
print("Execution plan:")
result_df.explain(extended=True)

# Collect metrics
print("Job metrics:")
for job in spark.sparkContext.statusTracker().getJobIdsForGroup():
    job_data = spark.sparkContext.statusTracker().getJobInfo(job)
    if job_data:
        print(f"Job ID: {job}, Num Tasks: {job_data.numTasks}, Num Active Tasks: {job_data.numActiveTasks}, Num Completed Tasks: {job_data.numCompletedTasks}")

# COMMAND ----------
# Clean up

# Check if AQE was used
print("\nChecking if AQE was used:")
spark.sql("SET spark.sql.adaptive.enabled").show(truncate=False)
spark.sql("SET spark.sql.adaptive.skewJoin.enabled").show(truncate=False)

# Check for signs of dynamic partition pruning in the explain plan
explain_string = result_df._jdf.queryExecution().explainString(True)
if "Dynamic Partition Pruning" in explain_string:
    print("Dynamic Partition Pruning was used despite being disabled.")
else:
    print("Dynamic Partition Pruning was not used, as expected.")

# Check for signs of skew join optimization in the explain plan
if "SkewedPartitioning" in explain_string:
    print("Skew join optimization was used despite being disabled.")
else:
    print("Skew join optimization was not used, as expected.")

spark.catalog.clearCache()