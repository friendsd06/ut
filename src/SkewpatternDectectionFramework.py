from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
import math

class SkewPatternDetectionFramework:
    def __init__(self, spark):
        self.spark = spark
        self.skew_threshold = 3  # Adjust this value based on your skew tolerance

    def detect_key_skew(self, df, key_col):
        key_counts = df.groupBy(key_col).count().cache()
        total_count = df.count()
        max_count = key_counts.agg(F.max("count")).collect()[0][0]
        skew_factor = max_count / (total_count / key_counts.count())

        # Get top 5 skewed keys
        top_skewed_keys = key_counts.orderBy(F.desc("count")).limit(5).collect()

        # Identify keys that need salting
        keys_to_salt = key_counts.filter(F.col("count") > (total_count / key_counts.count()) * self.skew_threshold) \
            .select(key_col).collect()

        key_counts.unpersist()

        return {
            "skew_factor": skew_factor,
            "max_count": max_count,
            "top_5_skewed_keys": top_skewed_keys,
            "keys_to_salt": [row[key_col] for row in keys_to_salt]
        }

    def detect_partition_skew(self, df):
        # Use F.spark_partition_id() to get the partition ID
        partition_counts = df.withColumn("partition_id", F.monotonically_increasing_id()) \
            .groupBy("partition_id") \
            .count() \
            .cache()

        total_count = df.count()
        max_count = partition_counts.agg(F.max("count")).collect()[0][0]
        skew_factor = max_count / (total_count / partition_counts.count())

        # Get partition with highest count
        highest_partition = partition_counts.orderBy(F.desc("count")).first()

        # Identify partitions that need repartitioning
        partitions_to_repartition = partition_counts.filter(F.col("count") > (total_count / partition_counts.count()) * self.skew_threshold) \
            .select("partition_id").collect()

        partition_counts.unpersist()

        return {
            "skew_factor": skew_factor,
            "max_count": max_count,
            "highest_partition": highest_partition,
            "partitions_to_repartition": [row["partition_id"] for row in partitions_to_repartition]
        }

    def detect_temporal_skew(self, df, time_col, window_duration="1 day"):
        temporal_counts = df.groupBy(F.window(F.col(time_col), window_duration)).count()
        total_count = df.count()
        max_count = temporal_counts.agg(F.max("count")).collect()[0][0]
        skew_factor = max_count / (total_count / temporal_counts.count())

        # Get top 5 skewed time windows
        top_skewed_windows = temporal_counts.orderBy(F.desc("count")).limit(5).collect()

        return {
            "skew_factor": skew_factor,
            "max_count": max_count,
            "top_5_skewed_windows": top_skewed_windows
        }

    def detect_value_skew(self, df, value_col):
        stats = df.select(F.stddev(value_col).alias("stddev"), F.avg(value_col).alias("mean")).collect()[0]
        cv = stats["stddev"] / stats["mean"] if stats["mean"] != 0 else 0
        return {"coefficient_of_variation": cv}

    def detect_null_skew(self, df):
        null_counts = df.select([F.sum(F.col(c).isNull().cast("int")).alias(c) for c in df.columns])
        total_count = df.count()
        null_percentages = null_counts.select([F.round(F.col(c) / total_count * 100, 2).alias(c) for c in df.columns])
        return null_percentages.collect()[0].asDict()

    def detect_cardinality_skew(self, df):
        cardinalities = df.agg(*[F.approx_count_distinct(c).alias(c) for c in df.columns])
        return cardinalities.collect()[0].asDict()

    def detect_outlier_skew(self, df, numeric_cols):
        stats = df.select([F.percentile_approx(F.col(c), [0.25, 0.75]).alias(f"{c}_quartiles") for c in numeric_cols])
        outlier_stats = {}
        for col in numeric_cols:
            q1, q3 = stats.select(f"{col}_quartiles").collect()[0][0]
            iqr = q3 - q1
            lower_bound = q1 - 1.5 * iqr
            upper_bound = q3 + 1.5 * iqr
            outlier_count = df.filter((F.col(col) < lower_bound) | (F.col(col) > upper_bound)).count()
            outlier_percentage = outlier_count / df.count() * 100
            outlier_stats[col] = {"outlier_percentage": outlier_percentage}
        return outlier_stats

    def detect_geographic_skew(self, df, geo_col):
        geo_counts = df.groupBy(geo_col).count()
        total_count = df.count()
        max_count = geo_counts.agg(F.max("count")).collect()[0][0]
        skew_factor = max_count / (total_count / geo_counts.count())

        # Get top 5 skewed geographic locations
        top_skewed_locations = geo_counts.orderBy(F.desc("count")).limit(5).collect()

        return {
            "skew_factor": skew_factor,
            "max_count": max_count,
            "top_5_skewed_locations": top_skewed_locations
        }

    def run_comprehensive_analysis(self, df, key_col, value_col, time_col=None, numeric_cols=None, geo_col=None):
        results = {
            "key_skew": self.detect_key_skew(df, key_col),
            "partition_skew": self.detect_partition_skew(df),
            "value_skew": self.detect_value_skew(df, value_col),
            "null_skew": self.detect_null_skew(df),
            "cardinality_skew": self.detect_cardinality_skew(df)
        }

        if time_col:
            results["temporal_skew"] = self.detect_temporal_skew(df, time_col)

        if numeric_cols:
            results["outlier_skew"] = self.detect_outlier_skew(df, numeric_cols)

        if geo_col:
            results["geographic_skew"] = self.detect_geographic_skew(df, geo_col)

        return results

    def print_recommendations(self, results):
        print("\nRecommendations:")

        # Key skew recommendations
        if results["key_skew"]["keys_to_salt"]:
            print(f"Consider salting the following keys: {results['key_skew']['keys_to_salt']}")
        print(f"Top 5 skewed keys: {results['key_skew']['top_5_skewed_keys']}")

        # Partition skew recommendations
        if results["partition_skew"]["partitions_to_repartition"]:
            print(f"Consider repartitioning data in partitions: {results['partition_skew']['partitions_to_repartition']}")
        print(f"Partition with highest count: {results['partition_skew']['highest_partition']}")

        # Other recommendations based on skew factors
        if results["key_skew"]["skew_factor"] > self.skew_threshold:
            print(f"High key skew detected. Consider using salting or custom partitioning strategies.")

        if results["partition_skew"]["skew_factor"] > self.skew_threshold:
            print(f"High partition skew detected. Consider repartitioning or adjusting partition strategy.")

        if "temporal_skew" in results and results["temporal_skew"]["skew_factor"] > self.skew_threshold:
            print(f"High temporal skew detected. Consider time-based partitioning or processing strategies.")

        if "geographic_skew" in results and results["geographic_skew"]["skew_factor"] > self.skew_threshold:
            print(f"High geographic skew detected. Consider region-based processing or custom partitioning.")

# Usage example
if __name__ == "__main__":
    spark = SparkSession.builder.appName("SkewPatternDetection").getOrCreate()

    # Assume we have a DataFrame 'df' loaded with our data
    df = spark.read.format("delta").load("/path/to/your/data")

    framework = SkewPatternDetectionFramework(spark)
    results = framework.run_comprehensive_analysis(
        df,
        key_col="customer_id",
        value_col="order_amount",
        time_col="order_date",
        numeric_cols=["order_amount", "item_count"],
        geo_col="country"
    )

    print("Skew Analysis Results:")
    for pattern, stats in results.items():
        print(f"{pattern}: {stats}")

    framework.print_recommendations(results)

    spark.stop()