from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, lit, rand, count, sum, percentile_approx, expr, udf, spark_partition_id, lag, window, avg, stddev
from pyspark.sql.types import StringType, DoubleType, StructType, StructField, IntegerType
from pyspark.sql.window import Window
from delta import DeltaTable
import matplotlib.pyplot as plt
import seaborn as sns
import pandas as pd
import json
import os

class AdvancedSkewDetectionFramework:
    def __init__(self, spark):
        self.spark = spark

    def detect_value_skew(self, df, key_col, value_col):
        stats = df.groupBy(key_col).agg(
            count("*").alias("count"),
            sum(value_col).alias("sum"),
            percentile_approx(value_col, [0.25, 0.5, 0.75, 0.95, 0.99], 10000).alias("percentiles")
        )

        total_count = df.count()
        total_sum = df.select(sum(value_col)).collect()[0][0]

        return stats.withColumn("count_percentage", col("count") / total_count * 100) \
            .withColumn("sum_percentage", col("sum") / total_sum * 100) \
            .withColumn("q1", col("percentiles")[0]) \
            .withColumn("median", col("percentiles")[1]) \
            .withColumn("q3", col("percentiles")[2]) \
            .withColumn("p95", col("percentiles")[3]) \
            .withColumn("p99", col("percentiles")[4]) \
            .withColumn("iqr", col("q3") - col("q1")) \
            .drop("percentiles")

    def detect_partition_skew(self, df):
        return df.groupBy(spark_partition_id().alias("partition_id")).count() \
            .withColumn("percentage", col("count") / df.count() * 100)

    def calculate_gini_coefficient(self, df, key_col):
        w = Window.orderBy(col("count").asc())
        gini_df = df.groupBy(key_col).count() \
            .withColumn("cum_count", sum("count").over(w)) \
            .withColumn("prev_cum_count", lag("cum_count", 1, 0).over(w)) \
            .withColumn("area", (col("cum_count") + col("prev_cum_count")) * col("count") / 2)

        total_count = df.count()
        total_area = gini_df.select(sum("area")).collect()[0][0]
        max_area = total_count * total_count / 2

        return 1 - (2 * total_area / max_area)

    def calculate_coefficient_of_variation(self, df, key_col):
        stats = df.groupBy(key_col).count().agg(
            avg("count").alias("mean"),
            stddev("count").alias("stddev")
        ).collect()[0]

        mean, stddev = stats["mean"], stats["stddev"]
        return (stddev / mean) if mean > 0 else 0

    def categorize_skew(self, skew_factor):
        if skew_factor <= 2:
            return "Low"
        elif skew_factor <= 5:
            return "Moderate"
        elif skew_factor <= 10:
            return "High"
        else:
            return "Severe"

    def detect_temporal_skew(self, df, key_col, time_col, window_duration="1 day"):
        return df.groupBy(window(col(time_col), window_duration), col(key_col)) \
            .count() \
            .groupBy("window") \
            .agg(
            count("*").alias("distinct_keys"),
            sum("count").alias("total_count"),
            max("count").alias("max_count")
        ) \
            .withColumn("skew_factor", col("max_count") / (col("total_count") / col("distinct_keys")))

    def detect_skew(self, df, key_col, value_col, time_col=None):
        value_skew = self.detect_value_skew(df, key_col, value_col)
        partition_skew = self.detect_partition_skew(df)
        gini = self.calculate_gini_coefficient(df, key_col)
        cv = self.calculate_coefficient_of_variation(df, key_col)

        max_count = value_skew.select(max("count")).collect()[0][0]
        avg_count = df.count() / value_skew.count()
        skew_factor = max_count / avg_count

        df_with_category = value_skew.withColumn("skew_category", udf(self.categorize_skew, StringType())(col("count") / avg_count))

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

    def generate_visual_report(self, skew_results, output_path):
        # Value Skew Visualization
        plt.figure(figsize=(12, 6))
        sns.barplot(x='key', y='count', data=skew_results['value_skew'].toPandas().sort_values('count', ascending=False).head(20))
        plt.title('Top 20 Keys by Count')
        plt.xticks(rotation=45)
        plt.tight_layout()
        plt.savefig(f"{output_path}/top_keys_by_count.png")
        plt.close()

        # Partition Skew Visualization
        plt.figure(figsize=(12, 6))
        sns.barplot(x='partition_id', y='count', data=skew_results['partition_skew'].toPandas().sort_values('count', ascending=False))
        plt.title('Data Distribution Across Partitions')
        plt.xticks(rotation=45)
        plt.tight_layout()
        plt.savefig(f"{output_path}/partition_distribution.png")
        plt.close()

        if skew_results['temporal_skew'] is not None:
            # Temporal Skew Visualization
            plt.figure(figsize=(12, 6))
            temporal_data = skew_results['temporal_skew'].toPandas()
            temporal_data['window'] = pd.to_datetime(temporal_data['window'].astype(str))
            sns.lineplot(x='window', y='skew_factor', data=temporal_data)
            plt.title('Temporal Skew Over Time')
            plt.xticks(rotation=45)
            plt.tight_layout()
            plt.savefig(f"{output_path}/temporal_skew.png")
            plt.close()

    def run_analysis(self, input_path, output_path, key_col, value_col, time_col=None):
        df = self.spark.read.format("delta").load(input_path)
        skew_results = self.detect_skew(df, key_col, value_col, time_col)
        recommendations = self.generate_recommendations(skew_results)

        # Save results
        self.save_results(skew_results, output_path)
        self.generate_visual_report(skew_results, output_path)

        with open(f"{output_path}/recommendations.txt", "w") as f:
            f.write("\n".join(recommendations))

        return skew_results, recommendations

    def save_results(self, results, output_path):
        # Save summary statistics
        summary = {
            "gini_coefficient": results["gini_coefficient"],
            "coefficient_of_variation": results["coefficient_of_variation"],
            "skew_factor": results["skew_factor"]
        }
        with open(f"{output_path}/summary.json", "w") as f:
            json.dump(summary, f, indent=2)

        # Save detailed results as Delta tables
        results["value_skew"].write.format("delta").mode("overwrite").save(f"{output_path}/value_skew")
        results["partition_skew"].write.format("delta").mode("overwrite").save(f"{output_path}/partition_skew")
        results["df_with_category"].write.format("delta").mode("overwrite").save(f"{output_path}/categorized_skew")

        if results["temporal_skew"] is not None:
            results["temporal_skew"].write.format("delta").mode("overwrite").save(f"{output_path}/temporal_skew")

# Usage
spark = SparkSession.builder.appName("AdvancedSkewDetection").getOrCreate()
framework = AdvancedSkewDetectionFramework(spark)
results, recommendations = framework.run_analysis(
    input_path="/path/to/input",
    output_path="/path/to/output",
    key_col="customer_id",
    value_col="order_amount",
    time_col="order_date"  # Optional
)

print("Analysis complete. Results and visualizations saved to output path.")
print("Recommendations:")
for rec in recommendations:
    print(f"- {rec}")