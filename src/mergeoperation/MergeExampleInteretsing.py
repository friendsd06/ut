# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from delta.tables import *
import random
from datetime import datetime, timedelta
import json

# COMMAND ----------

# Initialize Spark Session with Delta configurations
spark = SparkSession.builder \
    .appName("SmartCityTrafficMonitoring") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.databricks.delta.optimizeWrite.enabled", "true") \
    .config("spark.databricks.delta.autoCompact.enabled", "true") \
    .config("spark.databricks.delta.properties.defaults.enableChangeDataFeed", "true") \
    .getOrCreate()

# COMMAND ----------

# Define schema for traffic sensor data
sensor_schema = StructType([
    StructField("sensor_id", StringType(), False),
    StructField("location", StringType(), False),
    StructField("timestamp", TimestampType(), False),
    StructField("vehicle_count", IntegerType(), False),
    StructField("average_speed", DoubleType(), False),
    StructField("temperature", DoubleType(), True),
    StructField("weather_condition", StringType(), True)
])

# S3 paths for our Delta tables
s3_raw_data_path = "/mnt/your-s3-mount/traffic_raw_data"
s3_aggregated_data_path = "/mnt/your-s3-mount/traffic_aggregated_data"

# COMMAND ----------

# Function to generate sample sensor data
def generate_sensor_data(num_records, start_date, end_date):
    locations = ["Downtown", "Highway", "Suburb", "City Center", "Industrial Zone"]
    weather_conditions = ["Sunny", "Rainy", "Cloudy", "Snowy", "Foggy"]

    def random_datetime(start, end):
        return start + timedelta(seconds=random.randint(0, int((end - start).total_seconds())))

    data = []
    for _ in range(num_records):
        sensor_id = f"SENSOR_{random.randint(1, 100):03d}"
        location = random.choice(locations)
        timestamp = random_datetime(start_date, end_date)
        vehicle_count = random.randint(0, 100)
        average_speed = round(random.uniform(0, 120), 1)
        temperature = round(random.uniform(-10, 40), 1)
        weather_condition = random.choice(weather_conditions)

        data.append((sensor_id, location, timestamp, vehicle_count, average_speed, temperature, weather_condition))

    return spark.createDataFrame(data, sensor_schema)

# COMMAND ----------

# Simulate streaming data ingestion
def ingest_streaming_data(batch_id):
    print(f"Ingesting batch {batch_id}")
    end_date = datetime.now()
    start_date = end_date - timedelta(hours=1)

    batch_data = generate_sensor_data(10000, start_date, end_date)

    batch_data.write.format("delta") \
        .mode("append") \
        .option("mergeSchema", "true") \
        .save(s3_raw_data_path)

# Simulate 24 hours of data ingestion
for i in range(24):
    ingest_streaming_data(i)

# COMMAND ----------

# Read the raw data and display sample
raw_data = spark.read.format("delta").load(s3_raw_data_path)
display(raw_data.limit(10))

# COMMAND ----------

# Perform data quality checks
def perform_data_quality_checks(df):
    total_count = df.count()
    null_counts = df.select([count(when(col(c).isNull(), c)).alias(c) for c in df.columns]).collect()[0]

    print("Data Quality Report:")
    print(f"Total records: {total_count}")
    for column, null_count in zip(df.columns, null_counts):
        print(f"  - {column}: {null_count} null values ({null_count/total_count:.2%})")

    # Check for outliers in average_speed
    speed_stats = df.select(
        mean("average_speed").alias("mean_speed"),
        stddev("average_speed").alias("stddev_speed")
    ).collect()[0]

    outliers = df.filter((col("average_speed") > speed_stats.mean_speed + 3 * speed_stats.stddev_speed) |
                         (col("average_speed") < speed_stats.mean_speed - 3 * speed_stats.stddev_speed))

    print(f"\nNumber of speed outliers: {outliers.count()}")

perform_data_quality_checks(raw_data)

# COMMAND ----------

# Aggregate data hourly
aggregated_data = raw_data.groupBy("sensor_id", "location", window("timestamp", "1 hour")) \
    .agg(
    sum("vehicle_count").alias("total_vehicles"),
    avg("average_speed").alias("avg_speed"),
    avg("temperature").alias("avg_temperature"),
    mode("weather_condition").alias("dominant_weather")
)

# Write aggregated data to Delta table
aggregated_data.write.format("delta") \
    .mode("overwrite") \
    .partitionBy("location") \
    .save(s3_aggregated_data_path)

# COMMAND ----------

# Optimize the aggregated table
delta_table = DeltaTable.forPath(spark, s3_aggregated_data_path)
delta_table.optimize().executeCompaction()

# Z-order by sensor_id and window
delta_table.optimize().executeZOrderBy("sensor_id", "window")

# COMMAND ----------

# Query to find busiest hours in each location
busiest_hours = spark.read.format("delta").load(s3_aggregated_data_path) \
    .withColumn("hour", hour("window.start")) \
    .groupBy("location", "hour") \
    .agg(avg("total_vehicles").alias("avg_vehicles")) \
    .orderBy(desc("avg_vehicles")) \
    .groupBy("location") \
    .agg(
    first("hour").alias("busiest_hour"),
    first("avg_vehicles").alias("peak_vehicle_count")
)

display(busiest_hours)

# COMMAND ----------

# Time travel query: Compare traffic patterns with last week
current_data = spark.read.format("delta").load(s3_aggregated_data_path)
last_week_data = spark.read.format("delta").option("versionAsOf", 0).load(s3_aggregated_data_path)

traffic_comparison = current_data.alias("current") \
    .join(last_week_data.alias("last_week"),
          (col("current.sensor_id") == col("last_week.sensor_id")) &
          (col("current.location") == col("last_week.location")) &
          (hour(col("current.window.start")) == hour(col("last_week.window.start")))) \
    .select(
    "current.sensor_id",
    "current.location",
    hour("current.window.start").alias("hour"),
    col("current.total_vehicles").alias("current_vehicles"),
    col("last_week.total_vehicles").alias("last_week_vehicles"),
    ((col("current.total_vehicles") - col("last_week.total_vehicles")) / col("last_week.total_vehicles") * 100).alias("percent_change")
)

display(traffic_comparison.orderBy(abs(col("percent_change")).desc()).limit(10))

# COMMAND ----------

# Use Change Data Feed to track changes
changes = spark.read.format("delta") \
    .option("readChangeFeed", "true") \
    .option("startingVersion", 1) \
    .load(s3_raw_data_path)

print("Recent changes in raw data:")
display(changes.groupBy("_change_type").count())

# COMMAND ----------

# Create a dynamic view for real-time monitoring
spark.sql(f"CREATE OR REPLACE TEMPORARY VIEW traffic_monitoring AS SELECT * FROM delta.`{s3_aggregated_data_path}`")

# Example of a streaming query (Note: This will run indefinitely, you may want to limit its execution time in a notebook)
traffic_alert = spark.sql("""
    SELECT location, window.end as time, total_vehicles, avg_speed
    FROM traffic_monitoring
    WHERE total_vehicles > 80 AND avg_speed < 20
""")

query = traffic_alert.writeStream \
    .outputMode("append") \
    .format("console") \
    .start()

# Wait for some time to see results, then stop the query
# query.awaitTermination(60)  # Wait for 60 seconds
# query.stop()

# COMMAND ----------

# Generate a report on traffic patterns
def generate_traffic_report():
    report = {}

    # Overall statistics
    overall_stats = spark.read.format("delta").load(s3_aggregated_data_path) \
        .agg(
        avg("total_vehicles").alias("avg_vehicles_per_hour"),
        avg("avg_speed").alias("overall_avg_speed"),
        min("avg_temperature").alias("min_temperature"),
        max("avg_temperature").alias("max_temperature")
    ).collect()[0]

    report["overall_stats"] = {
        "avg_vehicles_per_hour": round(overall_stats["avg_vehicles_per_hour"], 2),
        "overall_avg_speed": round(overall_stats["overall_avg_speed"], 2),
        "temperature_range": f"{overall_stats['min_temperature']:.1f} - {overall_stats['max_temperature']:.1f}°C"
    }

    # Busiest locations
    busiest_locations = spark.read.format("delta").load(s3_aggregated_data_path) \
        .groupBy("location") \
        .agg(sum("total_vehicles").alias("total_traffic")) \
        .orderBy(desc("total_traffic")) \
        .limit(3) \
        .collect()

    report["busiest_locations"] = [{"location": row["location"], "total_traffic": row["total_traffic"]} for row in busiest_locations]

    # Weather impact
    weather_impact = spark.read.format("delta").load(s3_aggregated_data_path) \
        .groupBy("dominant_weather") \
        .agg(
        avg("total_vehicles").alias("avg_vehicles"),
        avg("avg_speed").alias("avg_speed")
    ) \
        .collect()

    report["weather_impact"] = [{
        "weather": row["dominant_weather"],
        "avg_vehicles": round(row["avg_vehicles"], 2),
        "avg_speed": round(row["avg_speed"], 2)
    } for row in weather_impact]

    return json.dumps(report, indent=2)

print(generate_traffic_report())