from pyspark.sql import SparkSession
from dbldatagen import DataGenerator, fakergen
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, DateType
from pyspark.sql.functions import col, array, expr, when

# Initialize Spark session
spark = SparkSession.builder.appName("SkewedLoanDataGeneration").getOrCreate()

# Generate reference data tables first (keeping these mostly the same)
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

# Define schema for main loan dataset (keeping this the same)
loan_schema = StructType([
    # ... (same as before)
])

# Generate main loan dataset with skew
loan_data_gen = (DataGenerator(spark, name="loan_data", rowcount=100000, partitions=4)
                 .withSchema(loan_schema)
                 .withColumnSpec("loan_id", fakergen("uuid4"))
                 # Skew 1: Heavy concentration on a few customers
                 .withColumnSpec("customer_id", expr(f"""
        case 
            when rand() < 0.3 then array({','.join(repr(id) for id in customer_ids[:5])})[int(rand() * 5)]
            else array({','.join(repr(id) for id in customer_ids)})[int(rand() * {len(customer_ids)})]
        end
    """))
                 # Skew 2: Uneven distribution of loan types
                 .withColumnSpec("loan_type", expr(f"""
        case
            when rand() < 0.5 then '{loan_types[0]}'
            when rand() < 0.8 then '{loan_types[1]}'
            else array({','.join(repr(lt) for lt in loan_types[2:])})[int(rand() * {len(loan_types) - 2})]
        end
    """))
                 .withColumnSpec("loan_amount", "double", minValue=1000, maxValue=1000000)
                 # Skew 3: Bimodal distribution for interest rates
                 .withColumnSpec("interest_rate", expr("case when rand() < 0.7 then rand() * 0.05 + 0.02 else rand() * 0.1 + 0.15 end"))
                 .withColumnSpec("term_months", "int", minValue=12, maxValue=360)
                 .withColumnSpec("origination_date", "date", begin="2020-01-01", end="2024-07-29")
                 .withColumnSpec("maturity_date", expr("date_add(origination_date, term_months * 30)"))
                 # Skew 4: Credit score distribution skewed towards higher scores
                 .withColumnSpec("credit_score", expr("300 + pow(rand(), 0.5) * 550"))
                 .withColumnSpec("annual_income", "double", minValue=20000, maxValue=1000000)
                 .withColumnSpec("debt_to_income_ratio", "double", minValue=0, maxValue=0.5)
                 .withColumnSpec("employment_status", fakergen("random_element", elements=("Employed", "Self-employed", "Unemployed", "Retired")))
                 .withColumnSpec("years_employed", "int", minValue=0, maxValue=40)
                 .withColumnSpec("home_ownership", fakergen("random_element", elements=("Own", "Rent", "Mortgage", "Other")))
                 .withColumnSpec("property_value", "double", minValue=50000, maxValue=2000000)
                 .withColumnSpec("loan_purpose", fakergen("random_element", elements=("Purchase", "Refinance", "Home Improvement", "Debt Consolidation", "Business", "Other")))
                 # Skew 5: Majority of loans are current
                 .withColumnSpec("loan_status", expr("case when rand() < 0.8 then 'Current' else array('Late', 'Default', 'Paid Off', 'In Grace Period')[int(rand() * 4)] end"))
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
                 # Skew 6: Concentration of loans in certain branches
                 .withColumnSpec("branch_id", expr(f"""
        case 
            when rand() < 0.4 then array({','.join(repr(id) for id in branch_ids[:3])})[int(rand() * 3)]
            else array({','.join(repr(id) for id in branch_ids)})[int(rand() * {len(branch_ids)})]
        end
    """))
                 # Skew 7: Some underwriters handle more loans than others
                 .withColumnSpec("underwriter_id", expr(f"""
        case 
            when rand() < 0.6 then array({','.join(repr(id) for id in underwriter_ids[:10])})[int(rand() * 10)]
            else array({','.join(repr(id) for id in underwriter_ids)})[int(rand() * {len(underwriter_ids)})]
        end
    """))
                 .withColumnSpec("co_borrower_id", expr(f"array({','.join(repr(id) for id in customer_ids)})[int(rand() * {len(customer_ids)})]"), percentNulls=0.7)
                 .withColumnSpec("guarantor_id", expr(f"array({','.join(repr(id) for id in customer_ids)})[int(rand() * {len(customer_ids)})]"), percentNulls=0.9)
                 # Skew 8: Uneven distribution of insurance types
                 .withColumnSpec("insurance_type", expr(f"""
        case
            when rand() < 0.5 then '{insurance_types[0]}'
            when rand() < 0.8 then '{insurance_types[1]}'
            else array({','.join(repr(it) for it in insurance_types[2:])})[int(rand() * {len(insurance_types) - 2})]
        end
    """), percentNulls=0.4)
                 .withColumnSpec("insurance_premium", "double", minValue=0, maxValue=5000, percentNulls=0.4)
                 )

# Generate the main loan dataset
loan_df = loan_data_gen.build()

# Display sample data and skew statistics
print("Sample data:")
loan_df.show(5)

print("\nSkew statistics:")
print("Customer concentration:")
loan_df.groupBy("customer_id").count().orderBy(col("count").desc()).show(5)

print("Loan type distribution:")
loan_df.groupBy("loan_type").count().orderBy(col("count").desc()).show()

print("Interest rate distribution:")
loan_df.select((col("interest_rate") * 100).cast("int").alias("interest_rate_bucket"))
.groupBy("interest_rate_bucket")
.count()
.orderBy("interest_rate_bucket")
.show()

print("Credit score distribution:")
loan_df.select((col("credit_score") / 50).cast("int") * 50 .alias("credit_score_bucket"))
.groupBy("credit_score_bucket")
.count()
.orderBy("credit_score_bucket")
.show()

print("Loan status distribution:")
loan_df.groupBy("loan_status").count().orderBy(col("count").desc()).show()

print("Branch concentration:")
loan_df.groupBy("branch_id").count().orderBy(col("count").desc()).show(5)

print("Underwriter workload:")
loan_df.groupBy("underwriter_id").count().orderBy(col("count").desc()).show(5)

print("Insurance type distribution:")
loan_df.groupBy("insurance_type").count().orderBy(col("count").desc()).show()