from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType, ArrayType, StructType
from pyspark.sql.functions import col, lit, when, array, struct

# Initialize Spark session
spark = SparkSession.builder \
    .appName("Generate 10 Source and Target DataFrames") \
    .getOrCreate()

# Define Schema for Test Data
schema = StructType([
    StructField("id", IntegerType(), False),
    StructField("name", StringType(), True),
    StructField("age", IntegerType(), True),
    StructField("loan_amount", DoubleType(), True),
    StructField("interest_rate", DoubleType(), True),
    StructField("loan_type", StringType(), True),
    StructField("loan_status", StringType(), True),
    StructField("disbursed_date", StringType(), True),
    StructField("due_date", StringType(), True),
    StructField("payments", ArrayType(DoubleType()), True)
])

# Define Base Data for Source DataFrames
base_data = [
    (1, "Alice", 30, 10000.0, 5.5, "personal", "active", "2023-01-01", "2024-01-01", [500.0, 500.0]),
    (2, "Bob", 45, 15000.0, 3.2, "auto", "closed", "2022-06-01", "2023-06-01", [750.0, 750.0]),
    (3, "Charlie", 35, 20000.0, 4.8, "mortgage", "default", "2021-12-01", "2022-12-01", [1000.0, 1000.0]),
]

# Create Source DataFrames
source_dfs = [spark.createDataFrame(base_data, schema) for _ in range(10)]

# Generate Target DataFrames with Different Scenarios
target_dfs = []

# Scenario 1: Identical Data
target_dfs.append(source_dfs[0])

# Scenario 2: Single Field Mismatch (loan_amount)
target_dfs.append(
    source_dfs[1].withColumn("loan_amount", when(col("id") == 1, 10500.0).otherwise(col("loan_amount")))
)

# Scenario 3: Nested Field Change (payments array)
target_dfs.append(
    source_dfs[2].withColumn("payments", when(col("id") == 2, array(lit(800.0), lit(750.0))).otherwise(col("payments")))
)

# Scenario 4: Missing Array Element (payments array)
target_dfs.append(
    source_dfs[3].withColumn("payments", when(col("id") == 3, array(lit(1000.0))).otherwise(col("payments")))
)

# Scenario 5: Date Format Change (disbursed_date)
target_dfs.append(
    source_dfs[4].withColumn("disbursed_date", when(col("id") == 1, lit("01-01-2023")).otherwise(col("disbursed_date")))
)

# Scenario 6: Null Value in Non-Nested Field (loan_type)
target_dfs.append(
    source_dfs[5].withColumn("loan_type", when(col("id") == 2, lit(None)).otherwise(col("loan_type")))
)

# Scenario 7: Extra Field in Target (extra_info)
schema_with_extra = schema.add(StructField("extra_info", StringType(), True))
data_with_extra = [Row(**row.asDict(), extra_info="extra") for row in source_dfs[6].collect()]
target_dfs.append(spark.createDataFrame(data_with_extra, schema_with_extra))

# Scenario 8: Partial Null in Array Field (payments)
target_dfs.append(
    source_dfs[7].withColumn("payments", when(col("id") == 1, array(lit(500.0), lit(None))).otherwise(col("payments")))
)

# Scenario 9: String Field Change (loan_status)
target_dfs.append(
    source_dfs[8].withColumn("loan_status", when(col("id") == 3, "settled").otherwise(col("loan_status")))
)

# Scenario 10: Data Type Change (loan_amount as String for one row)
target_dfs.append(
    source_dfs[9].withColumn("loan_amount", when(col("id") == 2, lit("15000.0")).otherwise(col("loan_amount")))
)

# Display the source and target DataFrames for each scenario
for i, (source_df, target_df) in enumerate(zip(source_dfs, target_dfs), start=1):
    print(f"--- Scenario {i} Source ---")
    source_df.show(truncate=False)

    print(f"--- Scenario {i} Target ---")
    target_df.show(truncate=False)
