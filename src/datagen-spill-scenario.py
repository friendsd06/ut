from pyspark.sql import SparkSession
from dbldatagen import DataGenerator, fakergen
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, DateType, ArrayType
from pyspark.sql.functions import col, array, expr, when, struct

# Initialize Spark session
spark = SparkSession.builder.appName("SpillInducingLoanDataGeneration").getOrCreate()

# Generate main loan dataset with spill-inducing characteristics
loan_data_gen = (DataGenerator(spark, name="loan_data", rowcount=1000000, partitions=100)
                 .withColumn("loan_id", fakergen("uuid4"))
                 .withColumn("customer_id", expr(f"""
        case 
            when rand() < 0.3 then array({','.join(repr(f'CUST{i}') for i in range(10))})[int(rand() * 10)]
            else concat('CUST', cast(rand() * 1000000 as int))
        end
    """))
                 .withColumn("loan_type", fakergen("random_element", elements=("Personal", "Mortgage", "Auto", "Business", "Student")))
                 .withColumn("loan_amount", expr("case when rand() < 0.7 then rand() * 50000 + 10000 else rand() * 950000 + 50000 end"))
                 .withColumn("interest_rate", expr("case when rand() < 0.7 then rand() * 0.05 + 0.02 else rand() * 0.1 + 0.15 end"))
                 .withColumn("term_months", "int", minValue=12, maxValue=360)
                 .withColumn("origination_date", "date", begin="2010-01-01", end="2024-07-29")
                 .withColumn("credit_score", expr("300 + pow(rand(), 0.5) * 550"))
                 .withColumn("annual_income", "double", minValue=20000, maxValue=1000000)
                 # High cardinality column for GROUP BY operations
                 .withColumn("transaction_id", expr("uuid()"))
                 # Wide table scenario - adding 100 additional columns
                 .withColumns({f"additional_detail_{i}": expr("rand()") for i in range(100)})
                 # Complex nested structure for aggregations
                 .withColumn("loan_history", expr("""
        array(
            struct(date_sub(current_date(), int(rand() * 1000)) as date, rand() * 1000 as amount, 
                   array('late', 'on_time', 'prepaid')[int(rand() * 3)] as status)
        )
    """))
                 # High distinct count column
                 .withColumn("daily_balance", expr("struct(date_sub(current_date(), int(rand() * 1000)) as date, rand() * loan_amount as balance)"))
                 # Uneven data distribution for sort operations
                 .withColumn("payment_date", expr("date_add('2023-01-01', int(pow(rand(), 3) * 365))"))
                 .withColumn("branch_id", expr(f"""
        case 
            when rand() < 0.4 then array('BR001', 'BR002', 'BR003')[int(rand() * 3)]
            else concat('BR', lpad(cast(rand() * 1000 as int), 3, '0'))
        end
    """))
                 .withColumn("underwriter_id", expr(f"""
        case 
            when rand() < 0.6 then array('UW001', 'UW002', 'UW003', 'UW004', 'UW005')[int(rand() * 5)]
            else concat('UW', lpad(cast(rand() * 1000 as int), 3, '0'))
        end
    """))
                 )

# Generate the main loan dataset
loan_df = loan_data_gen.build()

# Create a separate transactions table with skewed join conditions
transaction_data_gen = (DataGenerator(spark, name="transaction_data", rowcount=10000000, partitions=100)
                        .withColumn("transaction_id", fakergen("uuid4"))
                        .withColumn("loan_id", expr(f"""
        case 
            when rand() < 0.5 then array({','.join(repr(row.loan_id) for row in loan_df.select("loan_id").limit(10).collect())})[int(rand() * 10)]
            else array({','.join(repr(row.loan_id) for row in loan_df.select("loan_id").limit(1000).collect())})[int(rand() * 1000)]
        end
    """))
                        .withColumn("transaction_amount", "double", minValue=10, maxValue=10000)
                        .withColumn("transaction_date", "date", begin="2010-01-01", end="2024-07-29")
                        )

transaction_df = transaction_data_gen.build()

# Create a loan_tags table that might lead to cartesian products
loan_tags_gen = (DataGenerator(spark, name="loan_tags", rowcount=1000000, partitions=10)
                 .withColumn("tag_id", fakergen("uuid4"))
                 .withColumn("loan_id", expr(f"""
        array({','.join(repr(row.loan_id) for row in loan_df.select("loan_id").limit(100000).collect())})[int(rand() * 100000)]
    """))
                 .withColumn("tag", fakergen("word"))
                 )

loan_tags_df = loan_tags_gen.build()

# Create a loan_terms table just over the broadcast join threshold
loan_terms_gen = (DataGenerator(spark, name="loan_terms", rowcount=100000, partitions=1)
                  .withColumn("term_id", fakergen("uuid4"))
                  .withColumn("term_description", fakergen("sentence"))
                  .withColumn("term_category", fakergen("random_element", elements=("Standard", "Special", "Promotional", "Regulatory")))
                  )

loan_terms_df = loan_terms_gen.build()

# Display sample data and statistics
print("Loan data sample:")
loan_df.show(5)

print("\nTransaction data sample:")
transaction_df.show(5)

print("\nLoan tags sample:")
loan_tags_df.show(5)

print("\nLoan terms sample:")
loan_terms_df.show(5)

print("\nData distribution statistics:")
loan_df.groupBy("loan_type").count().orderBy(col("count").desc()).show()
loan_df.groupBy("branch_id").count().orderBy(col("count").desc()).show(5)
transaction_df.groupBy("loan_id").count().orderBy(col("count").desc()).show(5)