from pyspark.sql import SparkSession
from pyspark.sql.functions import col, rand, randn, when, lit, date_add, current_date, struct
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, DateType, ArrayType
from functools import reduce
import time

# Initialize Spark Session with limited driver memory
spark = SparkSession.builder.appName("UnionMemoryErrorDemo") \
    .config("spark.driver.memory", "4g") \
    .config("spark.sql.shuffle.partitions", "10") \
    .getOrCreate()

# Define a complex schema for loan application data
schema = StructType([
    StructField("application_id", StringType(), False),
    StructField("applicant_info", StructType([
        StructField("name", StringType(), False),
        StructField("age", IntegerType(), False),
        StructField("address", StringType(), False)
    ]), False),
    StructField("loan_details", StructType([
        StructField("amount", DoubleType(), False),
        StructField("term", IntegerType(), False),
        StructField("interest_rate", DoubleType(), False)
    ]), False),
    StructField("financial_info", StructType([
        StructField("annual_income", DoubleType(), False),
        StructField("debt_to_income_ratio", DoubleType(), False),
        StructField("credit_score", IntegerType(), False)
    ]), False),
    StructField("application_date", DateType(), False),
    StructField("historical_payments", ArrayType(DoubleType()), False)
])

# Function to create a large, complex DataFrame
def create_large_loan_df(id, rows=500000):  # Increased to 500,000 rows
    return spark.range(rows).select(
        col("id").cast(StringType()).alias("application_id"),
        struct(
            lit(f"Applicant {id}").alias("name"),
            (rand() * 40 + 20).cast(IntegerType()).alias("age"),
            lit("123 Main St").alias("address")
        ).alias("applicant_info"),
        struct(
            (rand() * 1000000 + 10000).alias("amount"),
            when(rand() > 0.5, 360).otherwise(180).alias("term"),
            (rand() * 5 + 3).alias("interest_rate")
        ).alias("loan_details"),
        struct(
            (rand() * 200000 + 30000).alias("annual_income"),
            (rand() * 0.4 + 0.1).alias("debt_to_income_ratio"),
            (randn() * 100 + 650).cast(IntegerType()).alias("credit_score")
        ).alias("financial_info"),
        date_add(current_date(), -(rand() * 365).cast(IntegerType())).alias("application_date"),
        (array([rand() * 1000 for _ in range(12)])).alias("historical_payments")
    )

print("Creating large loan application DataFrames...")
df1 = create_large_loan_df(1)
df2 = create_large_loan_df(2)
df3 = create_large_loan_df(3)
dataframes = [df1, df2, df3]

# Function to time and execute union operations
def time_operation(name, operation, dfs):
    print(f"\n{name}:")
    start = time.time()
    try:
        result = operation(dfs)
        count = result.count()
        duration = time.time() - start
        print(f"Total applications: {count}, Time: {duration:.2f} seconds")
        result.show(2, truncate=True)
    except Exception as e:
        print(f"Error occurred: {str(e)}")

# 1. Python's reduce function (likely to cause memory error)
time_operation("1. Python's reduce function",
               lambda dfs: reduce(lambda a, b: a.union(b), dfs),
               dataframes)

# 2. Spark's native union
time_operation("2. Spark's native union",
               lambda dfs: dfs[0].unionAll(dfs[1:]),
               dataframes)

# 3. unionByName
time_operation("3. unionByName",
               lambda dfs: reduce(lambda a, b: a.unionByName(b, allowMissingColumns=True), dfs),
               dataframes)

# 4. Spark's reduce
time_operation("4. Spark's reduce",
               lambda dfs: spark.createDataFrame(spark.sparkContext.emptyRDD(), dfs[0].schema).union(dfs),
               dataframes)

print("\nSpark UI URL:", spark.sparkContext.uiWebUrl)
spark.stop()

//
Why Python's reduce Function Can Cause Memory Errors in Spark
Python's reduce function, when used with Spark DataFrames, can lead to memory errors due to several factors:

Driver-Side Execution:

The Python reduce function runs on the driver node, not distributed across the cluster.
It pulls data to the driver for processing, which can overwhelm the driver's memory.


Serialization Overhead:

Data must be serialized from Spark's JVM to Python's runtime and back.
This process can be memory-intensive, especially for large datasets.


Lack of Lazy Evaluation:

Python's reduce eagerly evaluates each step, preventing Spark from optimizing the entire operation.
This can lead to unnecessary intermediate data materialization.


Accumulation of Intermediate Results:

Each iteration of reduce creates a new DataFrame, potentially accumulating large amounts of data in driver memory.


Loss of Distributed Processing:

The operation becomes bottlenecked by the driver's processing capacity and memory, losing Spark's distributed computing advantage.


Inefficient for Large Datasets:

As data size grows, the memory required on the driver increases, potentially exceeding available resources.


Spark Context Synchronization:

Each operation in the reduce function may require synchronization with the Spark context, adding overhead.