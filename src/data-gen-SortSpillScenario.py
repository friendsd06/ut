from pyspark.sql import SparkSession
from dbldatagen import DataGenerator, fakergen
from pyspark.sql.functions import col, expr, when

# Initialize Spark session
spark = SparkSession.builder.appName("SortSpillScenario").getOrCreate()

# Set a relatively low sort memory threshold to increase the likelihood of spills
spark.conf.set("spark.sql.shuffle.partitions", "200")
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")  # Disable broadcast joins
spark.conf.set("spark.sql.adaptive.enabled", "false")  # Disable adaptive query execution for this example

# Generate a large dataset with skewed data for sorting
data_gen = (DataGenerator(spark, name="large_skewed_dataset", rowcount=100_000_000, partitions=100)
            .withColumn("id", expr("uuid()"))
            .withColumn("category", expr("""
        case
            when rand() < 0.7 then array('A', 'B', 'C')[int(rand() * 3)]
            else array('D', 'E', 'F', 'G', 'H', 'I', 'J')[int(rand() * 7)]
        end
    """))
            .withColumn("timestamp", expr("date_sub(current_timestamp(), int(rand() * 365 * 3))"))  # Last 3 years
            .withColumn("value", expr("""
        case
            when rand() < 0.8 then rand() * 1000
            when rand() < 0.95 then rand() * 10000
            else rand() * 1000000
        end
    """))
            # Adding a column with long string values to increase row size
            .withColumn("description", expr("""
        concat_ws(' ', 
            array_repeat(array('long', 'string', 'to', 'increase', 'row', 'size', 'and', 'memory', 'usage')[int(rand() * 9)], 
            20 + int(rand() * 30))
        )
    """))
            )

# Build the dataset
df = data_gen.build()

# Cache the dataframe to simulate a real-world scenario where the data might be reused
df.cache()
df.count()  # Force caching

print("Dataset generated and cached. Sample data:")
df.show(5, truncate=False)

print("\nDataset statistics:")
df.describe().show()

print("\nCategory distribution:")
df.groupBy("category").count().orderBy(col("count").desc()).show()

# Perform a large sort operation that's likely to cause spills
print("\nPerforming large sort operation...")
sorted_df = df.sort(col("category"), col("timestamp").desc(), col("value").desc())

# Collect a small sample of the sorted data to force the sort operation
sorted_sample = sorted_df.limit(1000).collect()

print("\nSort operation completed. Sample of sorted data:")
spark.createDataFrame(sorted_sample).show(20, truncate=False)

# In a real scenario, you might want to save the sorted data or perform further operations
# sorted_df.write.parquet("path/to/sorted_data")

# Clean up
spark.catalog.clearCache()