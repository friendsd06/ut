from pyspark.sql import SparkSession
from dbldatagen import DataGenerator
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, DateType
from pyspark.sql.functions import expr, rand, concat, lit

# Initialize Spark session
spark = SparkSession.builder.appName("LoanDataGeneration").getOrCreate()

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

# Generate main loan dataset
loan_data_gen = (DataGenerator(spark, name="loan_data", rows=100000, partitions=4)
                 .withSchema(loan_schema)
                 .withIdOutput()
                 .withColumn("customer_id", "string", expr="uuid()")
                 .withColumn("loan_type", "string", values=["Personal", "Mortgage", "Auto", "Business", "Student"])
                 .withColumn("loan_amount", "double", minValue=1000, maxValue=1000000)
                 .withColumn("interest_rate", "double", minValue=0.01, maxValue=0.25)
                 .withColumn("term_months", "integer", minValue=12, maxValue=360)
                 .withColumn("origination_date", "date", begin="2020-01-01", end="2024-07-29")
                 .withColumn("maturity_date", expr="date_add(origination_date, term_months * 30)")
                 .withColumn("credit_score", "integer", minValue=300, maxValue=850)
                 .withColumn("annual_income", "double", minValue=20000, maxValue=1000000)
                 .withColumn("debt_to_income_ratio", "double", minValue=0, maxValue=0.5)
                 .withColumn("employment_status", "string", values=["Employed", "Self-employed", "Unemployed", "Retired"])
                 .withColumn("years_employed", "integer", minValue=0, maxValue=40)
                 .withColumn("home_ownership", "string", values=["Own", "Rent", "Mortgage", "Other"])
                 .withColumn("property_value", "double", minValue=50000, maxValue=2000000)
                 .withColumn("loan_purpose", "string", values=["Purchase", "Refinance", "Home Improvement", "Debt Consolidation", "Business", "Other"])
                 .withColumn("loan_status", "string", values=["Current", "Late", "Default", "Paid Off", "In Grace Period"])
                 .withColumn("payment_frequency", "string", values=["Monthly", "Bi-weekly", "Weekly"])
                 .withColumn("monthly_payment", expr="loan_amount * (interest_rate / 12) / (1 - power(1 + (interest_rate / 12), -term_months))")
                 .withColumn("total_payments_made", "integer", minValue=0, maxValue=360)
                 .withColumn("remaining_balance", expr="loan_amount - (monthly_payment * total_payments_made)")
                 .withColumn("last_payment_date", expr="date_sub(current_date(), int(rand() * 30))")
                 .withColumn("next_payment_date", expr="date_add(last_payment_date, 30)")
                 .withColumn("days_past_due", "integer", minValue=0, maxValue=90)
                 .withColumn("times_30_days_late", "integer", minValue=0, maxValue=10)
                 .withColumn("times_60_days_late", "integer", minValue=0, maxValue=5)
                 .withColumn("times_90_days_late", "integer", minValue=0, maxValue=3)
                 .withColumn("collateral_type", "string", values=["Real Estate", "Vehicle", "Securities", "None"])
                 .withColumn("collateral_value", "double", minValue=0, maxValue=2000000)
                 .withColumn("loan_to_value_ratio", expr="loan_amount / collateral_value")
                 .withColumn("origination_fee", expr="loan_amount * 0.01")
                 .withColumn("application_date", expr="date_sub(origination_date, int(rand() * 30))")
                 .withColumn("approval_date", expr="date_add(application_date, int(rand() * 14))")
                 .withColumn("funding_date", expr="date_add(approval_date, int(rand() * 7))")
                 .withColumn("branch_id", "string", expr="uuid()")
                 .withColumn("underwriter_id", "string", expr="uuid()")
                 .withColumn("co_borrower_id", "string", expr="uuid()", percentNulls=70)
                 .withColumn("guarantor_id", "string", expr="uuid()", percentNulls=90)
                 .withColumn("insurance_type", "string", values=["Life", "Disability", "Property", "None"], percentNulls=40)
                 .withColumn("insurance_premium", "double", minValue=0, maxValue=5000, percentNulls=40)
                 )

# Generate reference data tables
customer_data_gen = (DataGenerator(spark, name="customer_data", rows=50000, partitions=4)
                     .withIdOutput()
                     .withColumn("customer_id", "string", expr="uuid()")
                     .withColumn("first_name", "string", expr="concat('FirstName_', cast(rand() * 1000000 as int))")
                     .withColumn("last_name", "string", expr="concat('LastName_', cast(rand() * 1000000 as int))")
                     .withColumn("email", "string", expr="concat('user', cast(rand() * 1000000 as int), '@example.com')")
                     .withColumn("phone_number", "string", expr="concat('+1', cast(rand() * 1000000000 as int))")
                     )

branch_data_gen = (DataGenerator(spark, name="branch_data", rows=100, partitions=1)
                   .withIdOutput()
                   .withColumn("branch_id", "string", expr="uuid()")
                   .withColumn("branch_name", "string", expr="concat('Branch_', cast(rand() * 1000 as int))")
                   .withColumn("city", "string", expr="concat('City_', cast(rand() * 100 as int))")
                   .withColumn("state", "string", expr="concat('State_', cast(rand() * 50 as int))")
                   )

underwriter_data_gen = (DataGenerator(spark, name="underwriter_data", rows=500, partitions=1)
                        .withIdOutput()
                        .withColumn("underwriter_id", "string", expr="uuid()")
                        .withColumn("first_name", "string", expr="concat('FirstName_', cast(rand() * 1000 as int))")
                        .withColumn("last_name", "string", expr="concat('LastName_', cast(rand() * 1000 as int))")
                        .withColumn("employee_id", "string", expr="cast(100000 + cast(rand() * 900000 as int) as string)")
                        )

loan_type_data_gen = (DataGenerator(spark, name="loan_type_data", rows=5, partitions=1)
                      .withIdOutput()
                      .withColumn("loan_type", "string", values=["Personal", "Mortgage", "Auto", "Business", "Student"])
                      .withColumn("description", "string", expr="concat('Description for ', loan_type)")
                      .withColumn("max_term_months", "integer", minValue=12, maxValue=360)
                      )

insurance_data_gen = (DataGenerator(spark, name="insurance_data", rows=4, partitions=1)
                      .withIdOutput()
                      .withColumn("insurance_type", "string", values=["Life", "Disability", "Property", "None"])
                      .withColumn("description", "string", expr="concat('Description for ', insurance_type)")
                      .withColumn("coverage_percentage", "double", minValue=0.5, maxValue=1.0)
                      )

# Generate the datasets
loan_df = loan_data_gen.build()
customer_df = customer_data_gen.build()
branch_df = branch_data_gen.build()
underwriter_df = underwriter_data_gen.build()
loan_type_df = loan_type_data_gen.build()
insurance_df = insurance_data_gen.build()

# Show sample data
loan_df.show(5)
customer_df.show(5)
branch_df.show(5)
underwriter_df.show(5)
loan_type_df.show(5)
insurance_df.show(5)