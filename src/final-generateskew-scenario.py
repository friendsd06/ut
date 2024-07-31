# Configure Spark session to emphasize skew
spark = (SparkSession.builder
         .appName("LoanSkewScenario")
         .config("spark.sql.adaptive.enabled", "false")
         .config("spark.sql.shuffle.partitions", "200")
         .config("spark.sql.autoBroadcastJoinThreshold", "-1")
         .config("spark.default.parallelism", "100")
         .config("spark.sql.broadcastTimeout", "600")
         .getOrCreate())

# Read data from S3
customers_df = spark.read.csv("s3://your-bucket/customers", header=True, schema=customer_schema)
loans_df = spark.read.csv("s3://your-bucket/loans", header=True, schema=loan_schema)

# Cache dataframes
customers_df.cache()
loans_df.cache()

# Show distribution
print("Customer Distribution:")
customers_df.groupBy("type").count().show()

print("\nLoan Distribution:")
loan_distribution = loans_df.join(customers_df, "customer_id") \
    .groupBy("type") \
    .agg(
    sum_("loan_amount").alias("total_loan_amount"),
    count("*").alias("loan_count")
)
loan_distribution.show()

# Perform a complex operation to demonstrate skew
def complex_operation():
    print("\nPerforming complex join and aggregation...")
    start_time = time.time()
    result = loans_df.join(customers_df, "customer_id") \
        .groupBy("customer_id", "type", "name") \
        .agg(
        sum_("loan_amount").alias("total_loan_amount"),
        count("*").alias("loan_count"),
        expr("percentile(loan_amount, 0.5)").alias("median_loan_amount"),
        expr("stddev(loan_amount)").alias("loan_amount_stddev")
    ) \
        .orderBy(col("total_loan_amount").desc())

    result.show(10, truncate=False)
    end_time = time.time()
    print(f"Operation took {end_time - start_time:.2f} seconds")
    return result

result_df = complex_operation()

# Show execution plan
print("\nExecution Plan:")
result_df.explain(extended=True)

# Analyze skew in the result
print("\nAnalyzing result distribution:")
result_df.groupBy("type") \
    .agg(
    count("*").alias("customer_count"),
    sum_("total_loan_amount").alias("total_loans"),
    sum_("loan_count").alias("total_loan_count")
) \
    .show(truncate=False)

# Show the top 10 customers by loan amount
print("\nTop 10 customers by loan amount:")
result_df.orderBy(col("total_loan_amount").desc()).show(10, truncate=False)

# Perform another operation to show skew in action
def skewed_operation():
    print("\nPerforming operation to highlight skew...")
    start_time = time.time()
    skewed_result = loans_df.join(
        customers_df.select("customer_id", "name"),
        "customer_id"
    ).groupBy("name") \
        .agg(
        count("*").alias("loan_count"),
        sum_("loan_amount").alias("total_loan_amount"),
        expr("collect_list(loan_amount)").alias("all_loans")
    )

    skewed_result.show(10, truncate=False)
    end_time = time.time()
    print(f"Operation took {end_time - start_time:.2f} seconds")

skewed_operation()

# Clean up
spark.catalog.clearCache()