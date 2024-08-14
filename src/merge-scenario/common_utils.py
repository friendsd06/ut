from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, DateType
import random
from datetime import datetime, timedelta

def create_spark_session():
    return SparkSession.builder \
        .appName("LoanDataDeltaOperations") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.sql.shuffle.partitions", "400") \
        .config("spark.default.parallelism", "200") \
        .config("spark.sql.files.maxRecordsPerFile", "0") \
        .config("spark.databricks.delta.merge.optimizeInsertOnlyMerge.enabled", "true") \
        .getOrCreate()

def generate_loan_schema():
    base_fields = [
        StructField("loan_id", StringType(), False),
        StructField("cob_date", DateType(), False),
        StructField("slice", StringType(), False),
        StructField("customer_id", StringType(), True),
        StructField("loan_amount", DoubleType(), True),
        StructField("interest_rate", DoubleType(), True),
        StructField("loan_term", IntegerType(), True),
        StructField("loan_type", StringType(), True),
        StructField("credit_score", IntegerType(), True),
        StructField("annual_income", DoubleType(), True),
        StructField("debt_to_income_ratio", DoubleType(), True),
        StructField("employment_length", IntegerType(), True),
        StructField("home_ownership", StringType(), True),
        StructField("loan_purpose", StringType(), True),
        StructField("loan_status", StringType(), True),
        StructField("last_payment_date", DateType(), True),
        StructField("next_payment_date", DateType(), True),
        StructField("last_payment_amount", DoubleType(), True),
        StructField("remaining_balance", DoubleType(), True),
        StructField("delinquency_status", StringType(), True),
    ]
    additional_fields = [StructField(f"field_{i}", StringType(), True) for i in range(len(base_fields), 250)]
    return StructType(base_fields + additional_fields)

def generate_loan_data(spark, num_records, start_id=0):
    schema = generate_loan_schema()

    def generate_row(id):
        cob_date = datetime.now().date() - timedelta(days=random.randint(0, 29))
        return (
            f"LOAN-{id:010d}",
            cob_date,
            f"SLICE_{random.randint(1, 5)}",
            f"CUST-{random.randint(1, 1000000):07d}",
            random.uniform(1000, 1000000),
            random.uniform(1, 20),
            random.randint(12, 360),
            random.choice(["Personal", "Mortgage", "Auto", "Business"]),
            random.randint(300, 850),
            random.uniform(20000, 500000),
            random.uniform(0, 50),
            random.randint(0, 30),
            random.choice(["Own", "Rent", "Mortgage"]),
            random.choice(["Debt Consolidation", "Home Improvement", "Business", "Other"]),
            random.choice(["Current", "Late", "Default"]),
            cob_date - timedelta(days=random.randint(1, 30)),
            cob_date + timedelta(days=random.randint(1, 30)),
            random.uniform(100, 5000),
            random.uniform(1000, 1000000),
            random.choice(["None", "30 days", "60 days", "90+ days"]),
            *[f"Value_{random.randint(1, 1000)}" for _ in range(230)]
        )

    return spark.createDataFrame(
        [generate_row(i) for i in range(start_id, start_id + num_records)],
        schema
    )