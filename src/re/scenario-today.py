from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, ArrayType, DoubleType
from pyspark.sql import Row

# Initialize Spark session
spark = SparkSession.builder \
    .appName("Generate Test Scenarios") \
    .getOrCreate()

# Define Schema for Test Data
schema = StructType([
    StructField("id", IntegerType(), False),
    StructField("name", StringType(), True),
    StructField("details", StructType([
        StructField("age", IntegerType(), True),
        StructField("scores", ArrayType(IntegerType()), True)
    ]), True),
    StructField("salary", DoubleType(), True)
])

# Define Sample Data
source_data = [
    Row(id=1, name="Alice", details={"age": 30, "scores": [85, 90, 95]}, salary=50000),
    Row(id=2, name="Bob", details={"age": 25, "scores": [80, 85, 90]}, salary=45000),
    Row(id=3, name="Charlie", details={"age": 35, "scores": [90, 95, 100]}, salary=60000)
]

# Create a base DataFrame
source_df = spark.createDataFrame(source_data, schema)

# Step 2: Generate Variants for 20 Scenarios
target_scenarios = []

# Scenario 1: Identical data
target_scenarios.append(source_df)

# Scenario 2: Single Field Mismatch
target_scenarios.append(
    source_df.withColumn("name", when(col("id") == 2, "Bob Changed").otherwise(col("name")))
)

# Scenario 3: Nested Field Mismatch
target_scenario_3 = source_df.withColumn("details",
                                         when(col("id") == 1, struct(lit(31).alias("age"), col("details.scores"))).otherwise(col("details"))
                                         )
target_scenarios.append(target_scenario_3)

# Scenario 4: Additional Element in Array
target_scenario_4 = source_df.withColumn("details",
                                         when(col("id") == 2, struct(col("details.age"), array([80, 85, 90, 95]).alias("scores"))).otherwise(col("details"))
                                         )
target_scenarios.append(target_scenario_4)

# Scenario 5: Missing Element in Array
target_scenario_5 = source_df.withColumn("details",
                                         when(col("id") == 2, struct(col("details.age"), array([80, 85]).alias("scores"))).otherwise(col("details"))
                                         )
target_scenarios.append(target_scenario_5)

# Scenario 6: Data Type Change
target_scenarios.append(
    source_df.withColumn("salary", when(col("id") == 3, lit("60000")).otherwise(col("salary")))
)

# Scenario 7: Date Format Change
# Add a dummy date column for this scenario
source_df_with_date = source_df.withColumn("hire_date", lit("2023-01-01"))
target_scenario_7 = source_df_with_date.withColumn("hire_date",
                                                   when(col("id") == 1, lit("01-01-2023")).otherwise(col("hire_date"))
                                                   )
target_scenarios.append(target_scenario_7)

# Scenario 8: Field Value to Null
target_scenarios.append(
    source_df.withColumn("name", when(col("id") == 2, lit(None)).otherwise(col("name")))
)

# Scenario 9: Nested Field to Null
target_scenario_9 = source_df.withColumn("details",
                                         when(col("id") == 1, struct(lit(None).alias("age"), col("details.scores"))).otherwise(col("details"))
                                         )
target_scenarios.append(target_scenario_9)

# Scenario 10: Duplicate Rows
target_scenarios.append(
    source_df.union(source_df.filter(col("id") == 2))
)

# Scenario 11: Extra Row in Target
extra_row = Row(id=4, name="David", details={"age": 40, "scores": [88, 92]}, salary=70000)
target_scenarios.append(source_df.union(spark.createDataFrame([extra_row], schema)))

# Scenario 12: Extra Row in Source
extra_row_in_source = Row(id=5, name="Eve", details={"age": 28, "scores": [75, 80, 85]}, salary=40000)
source_df_with_extra = source_df.union(spark.createDataFrame([extra_row_in_source], schema))
target_scenarios.append(source_df)

# Scenario 13: Truncated String
target_scenarios.append(
    source_df.withColumn("name", when(col("id") == 3, lit("Char")).otherwise(col("name")))
)

# Scenario 14: Reordered Array Elements
target_scenario_14 = source_df.withColumn("details",
                                          when(col("id") == 1, struct(col("details.age"), array([95, 90, 85]).alias("scores"))).otherwise(col("details"))
                                          )
target_scenarios.append(target_scenario_14)

# Scenario 15: Partial Null Array
target_scenario_15 = source_df.withColumn("details",
                                          when(col("id") == 2, struct(col("details.age"), array([80, None, 90]).alias("scores"))).otherwise(col("details"))
                                          )
target_scenarios.append(target_scenario_15)

# Scenario 16: Nested Struct with Extra Field
# Add an extra field to the 'details' struct by creating a new schema version
schema_with_extra = StructType(schema.fields + [StructField("department", StringType(), True)])
extra_field_data = [Row(**row.asDict(), department="HR") for row in source_data]
target_scenarios.append(spark.createDataFrame(extra_field_data, schema_with_extra))

# Scenario 17: Different Numeric Precision
target_scenarios.append(
    source_df.withColumn("salary", when(col("id") == 3, lit(60000.123)).otherwise(col("salary")))
)

# Scenario 18: String Case Difference
target_scenarios.append(
    source_df.withColumn("name", when(col("id") == 1, lit("ALICE")).otherwise(col("name")))
)

# Scenario 19: Nested Array with Struct Elements Mismatch
target_scenario_19 = source_df.withColumn("details",
                                          when(col("id") == 2, struct(col("details.age"), array([80, 85, 99]).alias("scores"))).otherwise(col("details"))
                                          )
target_scenarios.append(target_scenario_19)

# Scenario 20: Inconsistent Value Format
target_scenarios.append(
    source_df.withColumn("salary", when(col("id") == 1, lit("50000")).otherwise(col("salary")))
)

# Step 3: Write Scenarios to Delta Files or Analyze Them
for i, target_df in enumerate(target_scenarios, start=1):
    target_df.write.format("delta").mode("overwrite").save(f"s3://your-s3-bucket/loan_data_scenario_{i}")

# Step 4: Optional - Run Comparison Code on Each Scenario
# Assuming `compare_delta_tables` function exists and is imported as shown previously
for i, target_df in enumerate(target_scenarios, start=1):
    result = compare_delta_tables(source_df, target_df, join_key="id")
    print(f"Scenario {i}:")
    result.show(truncate=False)
