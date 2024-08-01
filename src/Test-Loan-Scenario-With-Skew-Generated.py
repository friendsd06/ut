from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, lit, rand, explode, array, count, sum, avg, to_date, datediff, current_date, concat
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType, ArrayType, DateType

def create_spark_session():
    spark = SparkSession.builder.appName("SkewedLoanCustomerScenario").getOrCreate()
    spark.conf.set("spark.sql.shuffle.partitions", "200")
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")
    return spark

def define_schemas():
    customer_schema = StructType([
        StructField("customer_id", IntegerType(), False),
        StructField("name", StringType(), False),
        StructField("credit_score", IntegerType(), False),
        StructField("annual_income", IntegerType(), False)
    ])

    loan_schema = StructType([
        StructField("loan_id", IntegerType(), False),
        StructField("customer_id", IntegerType(), False),
        StructField("loan_amount", DoubleType(), False),
        StructField("interest_rate", DoubleType(), False),
        StructField("loan_term_days", IntegerType(), False),
        StructField("start_date", DateType(), False),
        StructField("payment_history", ArrayType(DoubleType()), False)
    ])

    return customer_schema, loan_schema

def generate_skewed_customer_data(spark, num_records, num_customers, skew_percentage):
    skewed_customer_id = 1
    df = spark.range(num_records)

    df = df.withColumn("customer_id",
                       when(rand() < skew_percentage, lit(skewed_customer_id))
                       .otherwise((col("id") % num_customers + 1).cast("int")))

    df = df.withColumn("name", concat(lit("Customer_"), col("customer_id").cast("string")))
    df = df.withColumn("credit_score",
                       when(col("customer_id") == skewed_customer_id, lit(800))
                       .otherwise((rand() * 300 + 500).cast("int")))
    df = df.withColumn("annual_income",
                       when(col("customer_id") == skewed_customer_id, lit(1000000))
                       .otherwise((rand() * 90000 + 10000).cast("int")))

    return df.select("customer_id", "name", "credit_score", "annual_income")

def generate_skewed_loan_data(spark, num_records, num_customers, skew_percentage):
    skewed_customer_id = 1
    df = spark.range(num_records)

    df = df.withColumn("loan_id", col("id") + 1)
    df = df.withColumn("customer_id",
                       when(rand() < skew_percentage, lit(skewed_customer_id))
                       .otherwise((col("id") % num_customers + 1).cast("int")))
    df = df.withColumn("loan_amount",
                       when(col("customer_id") == skewed_customer_id, rand() * 1000000 + 500000)
                       .otherwise(rand() * 50000 + 5000))
    df = df.withColumn("interest_rate", rand() * 10 + 2)
    df = df.withColumn("loan_term_days", (rand() * 1825 + 365).cast("int"))
    df = df.withColumn("start_date", to_date(current_date() - (rand() * 365).cast("int")))
    df = df.withColumn("payment_history", array([rand() for _ in range(12)]))

    return df.select("loan_id", "customer_id", "loan_amount", "interest_rate", "loan_term_days", "start_date", "payment_history")

def perform_analysis(df_customers, df_loans):
    joined_df = df_loans.join(df_customers, df_loans.customer_id == df_customers.customer_id)

    exploded_df = joined_df.withColumn("exploded_payment_history", explode(df_loans.payment_history))
    exploded_df = exploded_df.withColumn("days_since_start", datediff(current_date(), df_loans.start_date))

    result = exploded_df.groupBy(df_customers.customer_id, df_customers.name)
    result = result.agg(
        count("*").alias("num_loans"),
        sum(df_loans.loan_amount).alias("total_loan_amount"),
        avg(df_loans.interest_rate).alias("avg_interest_rate"),
        sum(when(col("exploded_payment_history") < 0.9, 1).otherwise(0)).alias("num_late_payments"),
        avg("days_since_start").alias("avg_loan_age")
    )

    return result

def main():
    spark = create_spark_session()
    customer_schema, loan_schema = define_schemas()

    num_customers = 1000000
    num_loans = 50000000
    customer_skew_percentage = 0.001
    loan_skew_percentage = 0.2

    df_customers = generate_skewed_customer_data(spark, num_customers, num_customers, customer_skew_percentage)
    df_loans = generate_skewed_loan_data(spark, num_loans, num_customers, loan_skew_percentage)

    df_customers = df_customers.alias("customers")
    df_loans = df_loans.alias("loans")
    df_customers.cache().count()

    result = perform_analysis(df_customers, df_loans)

    print("Starting computation...")
    result.explain(mode="extended")
    result_count = result.count()
    print(f"Result count: {result_count}")

    print("Loan distribution across customers:")
    loan_distribution = df_loans.groupBy("customer_id").count()
    loan_distribution.orderBy(col("count").desc()).show(10)

    print("Sample results:")
    result.orderBy(col("total_loan_amount").desc()).show(10)

if __name__ == "__main__":
    main()