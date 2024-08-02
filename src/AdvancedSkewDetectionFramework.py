from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, DoubleType, StructType, StructField, IntegerType
from pyspark.sql.window import Window

class AdvancedSkewDetectionFramework:
    def __init__(self, spark):
        self.spark = spark

    def detect_value_skew(self, df, key_col, value_col):
        stats = df.groupBy(key_col).agg(
            F.count("*").alias("count"),
            F.sum(value_col).alias("sum"),
            F.percentile_approx(value_col, [0.25, 0.5, 0.75, 0.95, 0.99], 10000).alias("percentiles")
        )

        total_count = df.count()
        total_sum = df.select(F.sum(value_col)).collect()[0][0]

        return stats.withColumn("count_percentage", F.col("count") / total_count * 100) \
            .withColumn("sum_percentage", F.col("sum") / total_sum * 100) \
            .withColumn("q1", F.col("percentiles")[0]) \
            .withColumn("median", F.col("percentiles")[1]) \
            .withColumn("q3", F.col("percentiles")[2]) \
            .withColumn("p95", F.col("percentiles")[3]) \
            .withColumn("p99", F.col("percentiles")[4]) \
            .withColumn("iqr", F.col("q3") - F.col("q1")) \
            .drop("percentiles")

    def detect_partition_skew(self, df):
        return df.groupBy(F.spark_partition_id().alias("partition_id")).count() \
            .withColumn("percentage", F.col("count") / df.count() * 100)

    def calculate_gini_coefficient(self, df, key_col):
        w = Window.orderBy(F.col("count").asc())
        gini_df = df.groupBy(key_col).count() \
            .withColumn("cum_count", F.sum("count").over(w)) \
            .withColumn("prev_cum_count", F.lag("cum_count", 1, 0).over(w)) \
            .withColumn("area", (F.col("cum_count") + F.col("prev_cum_count")) * F.col("count") / 2)

        total_count = df.count()
        total_area = gini_df.select(F.sum("area")).collect()[0][0]
        max_area = total_count * total_count / 2

        return 1 - (2 * total_area / max_area)

    def calculate_coefficient_of_variation(self, df, key_col):
        stats = df.groupBy(key_col).count().agg(
            F.avg("count").alias("mean"),
            F.stddev("count").alias("std_dev")
        ).collect()[0]

        mean, std_dev = stats["mean"], stats["std_dev"]
        return (std_dev / mean) if mean > 0 else 0

    def detect_temporal_skew(self, df, key_col, time_col, window_duration="1 day"):
        return df.groupBy(F.window(F.col(time_col), window_duration), F.col(key_col)) \
            .count() \
            .groupBy("window") \
            .agg(
            F.count("*").alias("distinct_keys"),
            F.sum("count").alias("total_count"),
            F.max("count").alias("max_count")
        ) \
            .withColumn("skew_factor", F.col("max_count") / (F.col("total_count") / F.col("distinct_keys")))

    def detect_skew(self, df, key_col, value_col, time_col=None):
        value_skew = self.detect_value_skew(df, key_col, value_col)
        partition_skew = self.detect_partition_skew(df)
        gini = self.calculate_gini_coefficient(df, key_col)
        cv = self.calculate_coefficient_of_variation(df, key_col)

        max_count = value_skew.select(F.max("count")).collect()[0][0]
        total_count = df.count()
        avg_count = total_count / value_skew.count()
        skew_factor = max_count / avg_count

        def categorize_skew(count):
            local_skew_factor = count / avg_count
            if local_skew_factor <= 2:
                return "Low"
            elif local_skew_factor <= 5:
                return "Moderate"
            elif local_skew_factor <= 10:
                return "High"
            else:
                return "Severe"

        categorize_skew_udf = F.udf(categorize_skew, StringType())
        df_with_category = value_skew.withColumn("skew_category", categorize_skew_udf(F.col("count")))

        temporal_skew = None
        if time_col:
            temporal_skew = self.detect_temporal_skew(df, key_col, time_col)

        return {
            "gini_coefficient": gini,
            "coefficient_of_variation": cv,
            "skew_factor": skew_factor,
            "value_skew": value_skew,
            "partition_skew": partition_skew,
            "df_with_category": df_with_category,
            "temporal_skew": temporal_skew
        }

    def generate_recommendations(self, skew_results):
        recommendations = []
        if skew_results["skew_factor"] > 10:
            recommendations.append("Consider data repartitioning or salting for highly skewed keys")
        if skew_results["gini_coefficient"] > 0.6:
            recommendations.append("Investigate causes of high data inequality and consider data balancing techniques")
        if skew_results["coefficient_of_variation"] > 1:
            recommendations.append("High variability in key counts. Consider using more uniform key distribution if possible")
        return recommendations

    def run_analysis(self, input_path, key_col, value_col, time_col=None):
        df = self.spark.read.format("delta").load(input_path)
        skew_results = self.detect_skew(df, key_col, value_col, time_col)
        recommendations = self.generate_recommendations(skew_results)
        return skew_results, recommendations

# Usage example
if __name__ == "__main__":
    # Initialize Spark Session
    spark = SparkSession.builder \
        .appName("AdvancedSkewDetection") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .getOrCreate()

    # Initialize the framework
    framework = AdvancedSkewDetectionFramework(spark)

    # Run the analysis
    results, recommendations = framework.run_analysis(
        input_path="/path/to/your/delta/table",
        key_col="your_key_column",
        value_col="your_value_column",
        time_col="your_time_column"  # Optional, remove if not needed
    )

    # Print results
    print("Skew Analysis Results:")
    print(f"Gini Coefficient: {results['gini_coefficient']}")
    print(f"Coefficient of Variation: {results['coefficient_of_variation']}")
    print(f"Skew Factor: {results['skew_factor']}")

    print("\nRecommendations:")
    for rec in recommendations:
        print(f"- {rec}")

    # Show sample of value skew results
    print("\nSample of Value Skew Results:")
    results['value_skew'].show(5)

    # Show sample of partition skew results
    print("\nSample of Partition Skew Results:")
    results['partition_skew'].show(5)

    # Stop Spark session
    spark.stop()