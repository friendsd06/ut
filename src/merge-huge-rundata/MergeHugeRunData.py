import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr, concat, lit, current_timestamp, when, date_add, current_date
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType, DateType, TimestampType
from delta.tables import DeltaTable

# ========================
# Spark Session Setup
# ========================

def create_spark_session():
    """Create and return a Spark session with Delta Lake configurations"""
    return SparkSession.builder \
        .appName("DeltaLakeMergePerformance") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .getOrCreate()

# ========================
# Schema Definition
# ========================

def create_schema():
    """Create and return the schema for our data"""
    base_fields = [
        StructField("id", IntegerType(), False),
        StructField("cob_date", DateType(), False),
        StructField("slice", StringType(), False),
        StructField("name", StringType(), True),
        StructField("value", DoubleType(), True),
        StructField("category", StringType(), True),
        StructField("timestamp", TimestampType(), True)
    ]

    # Add 44 additional attribute columns
    attribute_fields = [StructField(f"attr_{i}", StringType(), True) for i in range(1, 45)]

    return StructType(base_fields + attribute_fields)

# ========================
# Data Generation
# ========================

def generate_data(spark, num_records, start_id=0):
    """Generate sample data with the given number of records, distributed equally across 6 cob_dates"""
    df = spark.range(start_id, start_id + num_records) \
        .withColumn("cob_date", expr("date_sub(current_date(), (id % 6) + 1)")) \
        .withColumn("slice", concat(lit("SLICE_"), ((col("id") % 5) + 1).cast("string"))) \
        .withColumn("name", concat(lit("Name_"), col("id").cast("string"))) \
        .withColumn("value", (col("id") % 1000).cast("double")) \
        .withColumn("category", when(col("id") % 5 == 0, "A")
                    .when(col("id") % 5 == 1, "B")
                    .when(col("id") % 5 == 2, "C")
                    .when(col("id") % 5 == 3, "D")
                    .otherwise("E")) \
        .withColumn("timestamp", current_timestamp())

    # Add 44 attribute columns
    for i in range(1, 45):
        df = df.withColumn(f"attr_{i}", concat(lit(f"Attr{i}_"), (col("id") % 100).cast("string")))

    return df

# ========================
# Delta Table Operations
# ========================

def create_main_table(spark, table_path, num_records):
    """Create the main Delta table with the given number of records"""
    print(f"Creating main table with {num_records} records...")
    start_time = time.time()

    data = generate_data(spark, num_records)
    data.write.format("delta") \
        .partitionBy("cob_date") \
        .mode("overwrite") \
        .save(table_path)

    end_time = time.time()
    print(f"Main table created in {end_time - start_time:.2f} seconds")

def perform_merge(spark, table_path, update_data, scenario_name):
    """Perform merge operation and return execution time"""
    delta_table = DeltaTable.forPath(spark, table_path)

    start_time = time.time()

    delta_table.alias("main").merge(
        update_data.alias("updates"),
        "main.id = updates.id AND main.cob_date = updates.cob_date AND main.slice = updates.slice"
    ).whenMatchedUpdate(set={
        "value": "updates.value",
        "category": "updates.category",
        "timestamp": "updates.timestamp",
        "attr_1": "updates.attr_1",
        # ... add more columns as needed
        "attr_44": "updates.attr_44"
    }).whenNotMatchedInsert(values={
        "id": "updates.id",
        "cob_date": "updates.cob_date",
        "slice": "updates.slice",
        "name": "updates.name",
        "value": "updates.value",
        "category": "updates.category",
        "timestamp": "updates.timestamp",
        "attr_1": "updates.attr_1",
        # ... add more columns as needed
        "attr_44": "updates.attr_44"
    }).execute()

    end_time = time.time()
    execution_time = end_time - start_time

    print(f"{scenario_name} completed in {execution_time:.2f} seconds")
    return execution_time

# ========================
# Performance Testing
# ========================

def run_merge_scenarios(spark, table_path):
    """Run various merge scenarios and collect statistics"""
    scenarios = [
        ("Small Merge", 100),
        ("Medium Merge", 10000),
        ("Large Merge", 100000),
        ("Very Large Merge", 300000)
    ]

    stats = []
    for scenario_name, num_records in scenarios:
        update_data = generate_data(spark, num_records, start_id=20_000_000)  # Start ID after initial data
        execution_time = perform_merge(spark, table_path, update_data, scenario_name)
        stats.append((scenario_name, num_records, execution_time))

    return stats

# ========================
# Analysis and Reporting
# ========================

def display_stats(stats):
    """Display merge statistics"""
    print("\nMerge Performance Statistics:")
    print("-----------------------------")
    print("Scenario          | Records  | Execution Time (s)")
    print("-----------------|---------|-----------------")
    for scenario, records, time in stats:
        print(f"{scenario:<18} | {records:<8} | {time:.2f}")

def analyze_table_metrics(spark, table_path):
    """Analyze and display table metrics"""
    print("\nTable Metrics:")
    print("--------------")
    metrics = spark.sql(f"DESCRIBE DETAIL delta.`{table_path}`").collect()[0]
    print(f"Number of files: {metrics['numFiles']}")
    print(f"Size in bytes: {metrics['sizeInBytes']}")
    print(f"Number of partitions: {metrics['numPartitions']}")

    # Display partition distribution
    print("\nPartition Distribution:")
    spark.sql(f"SELECT cob_date, slice, count(*) as record_count FROM delta.`{table_path}` GROUP BY cob_date, slice ORDER BY cob_date, slice") \
        .show(10, truncate=False)

# ========================
# Main Execution
# ========================

def main():
    # Initialize Spark session
    spark = create_spark_session()

    # Define paths
    base_path = "/path/to/your/delta/table"
    main_table_path = f"{base_path}/main_table"

    try:
        # Create main table with 20 million records
        create_main_table(spark, main_table_path, 20_000_000)

        # Analyze initial table metrics
        analyze_table_metrics(spark, main_table_path)

        # Run merge scenarios
        merge_stats = run_merge_scenarios(spark, main_table_path)

        # Display merge statistics
        display_stats(merge_stats)

        # Analyze final table metrics
        analyze_table_metrics(spark, main_table_path)

    finally:
        # Stop Spark session
        spark.stop()

if __name__ == "__main__":
    main()