from pyspark.sql import SparkSession
from dbldatagen import DataGenerator, fakergen
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, DateType
from pyspark.sql.functions import col, array, expr

# Initialize Spark session
spark = SparkSession.builder.appName("LoanDataGeneration").getOrCreate()

# Generate reference data tables first
customer_data_gen = (DataGenerator(spark, name="customer_data", rowcount=50000, partitions=4)
                     .withColumn("customer_id", fakergen("uuid4"))
                     .withColumn("first_name", fakergen("first_name"))
                     .withColumn("last_name", fakergen("last_name"))
                     .withColumn("email", fakergen("email"))
                     .withColumn("phone_number", fakergen("phone_number"))
                     )

branch_data_gen = (DataGenerator(spark, name="branch_data", rowcount=100, partitions=1)
                   .withColumn("branch_id", fakergen("uuid4"))
                   .withColumn("branch_name", fakergen("company"))
                   .withColumn("city", fakergen("city"))
                   .withColumn("state", fakergen("state"))
                   )

underwriter_data_gen = (DataGenerator(spark, name="underwriter_data", rowcount=500, partitions=1)
                        .withColumn("underwriter_id", fakergen("uuid4"))
                        .withColumn("first_name", fakergen("first_name"))
                        .withColumn("last_name", fakergen("last_name"))
                        .withColumn("employee_id", fakergen("random_number", digits=6))
                        )

loan_type_data_gen = (DataGenerator(spark, name="loan_type_data", rowcount=5, partitions=1)
                      .withColumn("loan_type", fakergen("random_element", elements=("Personal", "Mortgage", "Auto", "Business", "Student")))
                      .withColumn("description", fakergen("sentence"))
                      .withColumn("max_term_months", "int", minValue=12, maxValue=360)
                      )

insurance_data_gen = (DataGenerator(spark, name="insurance_data", rowcount=4, partitions=1)
                      .withColumn("insurance_type", fakergen("random_element", elements=("Life", "Disability", "Property", "None")))
                      .withColumn("description", fakergen("sentence"))
                      .withColumn("coverage_percentage", "double", minValue=0.5, maxValue=1.0)
                      )

# Generate the reference datasets
customer_df = customer_data_gen.build()
branch_df = branch_data_gen.build()
underwriter_df = underwriter_data_gen.build()
loan_type_df = loan_type_data_gen.build()
insurance_df = insurance_data_gen.build()

# Collect the IDs and types from reference data
customer_ids = [row.customer_id for row in customer_df.select("customer_id").collect()]
branch_ids = [row.branch_id for row in branch_df.select("branch_id").collect()]
underwriter_ids = [row.underwriter_id for row in underwriter_df.select("underwriter_id").collect()]
loan_types = [row.loan_type for row in loan_type_df.select("loan_type").collect()]
insurance_types = [row.insurance_type for row in insurance_df.select("insurance_type").collect()]

# Define schema for main loan dataset
loan_schema = StructType([
    StructField("loan_id", StringType(), False),
    StructField("customer_id", StringType(), False),
    StructField("loan_type", StringType(), False),
    StructField("loan_amount", DoubleType(), False),
    StructField("interest_rate", DoubleType(), False),
    StructField("term_months", IntegerType(), False),
    StructField("origination_date", DateType(), False),
    StructField("maturity_date", DateType(), False),
    StructField("credit_score", IntegerType(), False),
    StructField("annual_income", DoubleType(), False),
    StructField("debt_to_income_ratio", DoubleType(), False),
    StructField("employment_status", StringType(), False),
    StructField("years_employed", IntegerType(), False),
    StructField("home_ownership", StringType(), False),
    StructField("property_value", DoubleType(), False),
    StructField("loan_purpose", StringType(), False),
    StructField("loan_status", StringType(), False),
    StructField("payment_frequency", StringType(), False),
    StructField("monthly_payment", DoubleType(), False),
    StructField("total_payments_made", IntegerType(), False),
    StructField("remaining_balance", DoubleType(), False),
    StructField("last_payment_date", DateType(), False),
    StructField("next_payment_date", DateType(), False),
    StructField("days_past_due", IntegerType(), False),
    StructField("times_30_days_late", IntegerType(), False),
    StructField("times_60_days_late", IntegerType(), False),
    StructField("times_90_days_late", IntegerType(), False),
    StructField("collateral_type", StringType(), False),
    StructField("collateral_value", DoubleType(), False),
    StructField("loan_to_value_ratio", DoubleType(), False),
    StructField("origination_fee", DoubleType(), False),
    StructField("application_date", DateType(), False),
    StructField("approval_date", DateType(), False),
    StructField("funding_date", DateType(), False),
    StructField("branch_id", StringType(), False),
    StructField("underwriter_id", StringType(), False),
    StructField("co_borrower_id", StringType(), True),
    StructField("guarantor_id", StringType(), True),
    StructField("insurance_type", StringType(), True),
    StructField("insurance_premium", DoubleType(), True)
])

# Generate main loan dataset with lookups to reference data
loan_data_gen = (DataGenerator(spark, name="loan_data", rowcount=100000, partitions=4)
                 .withSchema(loan_schema)
                 .withColumnSpec("loan_id", fakergen("uuid4"))
                 .withColumnSpec("customer_id", expr(f"array({','.join(repr(id) for id in customer_ids)})[int(rand() * {len(customer_ids)})]"))
                 .withColumnSpec("loan_type", expr(f"array({','.join(repr(lt) for lt in loan_types)})[int(rand() * {len(loan_types)})]"))
                 .withColumnSpec("loan_amount", "double", minValue=1000, maxValue=1000000)
                 .withColumnSpec("interest_rate", "double", minValue=0.01, maxValue=0.25)
                 .withColumnSpec("term_months", "int", minValue=12, maxValue=360)
                 .withColumnSpec("origination_date", "date", begin="2020-01-01", end="2024-07-29")
                 .withColumnSpec("maturity_date", expr="date_add(origination_date, term_months * 30)")
                 .withColumnSpec("credit_score", "int", minValue=300, maxValue=850)
                 .withColumnSpec("annual_income", "double", minValue=20000, maxValue=1000000)
                 .withColumnSpec("debt_to_income_ratio", "double", minValue=0, maxValue=0.5)
                 .withColumnSpec("employment_status", fakergen("random_element", elements=("Employed", "Self-employed", "Unemployed", "Retired")))
                 .withColumnSpec("years_employed", "int", minValue=0, maxValue=40)
                 .withColumnSpec("home_ownership", fakergen("random_element", elements=("Own", "Rent", "Mortgage", "Other")))
                 .withColumnSpec("property_value", "double", minValue=50000, maxValue=2000000)
                 .withColumnSpec("loan_purpose", fakergen("random_element", elements=("Purchase", "Refinance", "Home Improvement", "Debt Consolidation", "Business", "Other")))
                 .withColumnSpec("loan_status", fakergen("random_element", elements=("Current", "Late", "Default", "Paid Off", "In Grace Period")))
                 .withColumnSpec("payment_frequency", fakergen("random_element", elements=("Monthly", "Bi-weekly", "Weekly")))
                 .withColumnSpec("monthly_payment", expr("loan_amount * (interest_rate / 12) / (1 - power(1 + (interest_rate / 12), -term_months))"))
                 .withColumnSpec("total_payments_made", "int", minValue=0, maxValue=360)
                 .withColumnSpec("remaining_balance", expr("loan_amount - (monthly_payment * total_payments_made)"))
                 .withColumnSpec("last_payment_date", expr("date_sub(current_date(), int(rand() * 30))"))
                 .withColumnSpec("next_payment_date", expr("date_add(last_payment_date, 30)"))
                 .withColumnSpec("days_past_due", "int", minValue=0, maxValue=90)
                 .withColumnSpec("times_30_days_late", "int", minValue=0, maxValue=10)
                 .withColumnSpec("times_60_days_late", "int", minValue=0, maxValue=5)
                 .withColumnSpec("times_90_days_late", "int", minValue=0, maxValue=3)
                 .withColumnSpec("collateral_type", fakergen("random_element", elements=("Real Estate", "Vehicle", "Securities", "None")))
                 .withColumnSpec("collateral_value", "double", minValue=0, maxValue=2000000)
                 .withColumnSpec("loan_to_value_ratio", expr("loan_amount / collateral_value"))
                 .withColumnSpec("origination_fee", expr("loan_amount * 0.01"))
                 .withColumnSpec("application_date", expr("date_sub(origination_date, int(rand() * 30))"))
                 .withColumnSpec("approval_date", expr("date_add(application_date, int(rand() * 14))"))
                 .withColumnSpec("funding_date", expr("date_add(approval_date, int(rand() * 7))"))
                 .withColumnSpec("branch_id", expr(f"array({','.join(repr(id) for id in branch_ids)})[int(rand() * {len(branch_ids)})]"))
                 .withColumnSpec("underwriter_id", expr(f"array({','.join(repr(id) for id in underwriter_ids)})[int(rand() * {len(underwriter_ids)})]"))
                 .withColumnSpec("co_borrower_id", expr(f"array({','.join(repr(id) for id in customer_ids)})[int(rand() * {len(customer_ids)})]"), percentNulls=0.7)
                 .withColumnSpec("guarantor_id", expr(f"array({','.join(repr(id) for id in customer_ids)})[int(rand() * {len(customer_ids)})]"), percentNulls=0.9)
                 .withColumnSpec("insurance_type", expr(f"array({','.join(repr(it) for it in insurance_types)})[int(rand() * {len(insurance_types)})]"), percentNulls=0.4)
                 .withColumnSpec("insurance_premium", "double", minValue=0, maxValue=5000, percentNulls=0.4)
                 )

# Generate the main loan dataset
loan_df = loan_data_gen.build()

# Verify lookups
print("Sample data with lookups:")
loan_df.select("loan_id", "customer_id", "branch_id", "underwriter_id", "loan_type", "insurance_type").show(5)

# Verify that all values in the main dataset exist in the reference data
print("\nVerifying lookups:")
print(f"All customer_ids valid: {loan_df.join(customer_df, 'customer_id', 'left_anti').count() == 0}")
print(f"All branch_ids valid: {loan_df.join(branch_df, 'branch_id', 'left_anti').count() == 0}")
print(f"All underwriter_ids valid: {loan_df.join(underwriter_df, 'underwriter_id', 'left_anti').count() == 0}")
print(f"All loan_types valid: {loan_df.join(loan_type_df, 'loan_type', 'left_anti').count() == 0}")
print(f"All insurance_types valid: {loan_df.filter(col('insurance_type').isNotNull()).join(insurance_df, 'insurance_type', 'left_anti').count() == 0}")