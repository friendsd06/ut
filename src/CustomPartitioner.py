from pyspark import RDD, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import spark_partition_id, count

class SkewedPartitioner:
    def __init__(self, partitions):
        self.partitions = partitions

    def getPartition(self, key):
        if key == 1:  # Assuming 1 is our skewed key
            return 0  # Always put key 1 in the first partition
        else:
            return hash(key) % (self.partitions - 1) + 1

def create_skewed_rdd(sc: SparkContext, num_rows=1000000, num_partitions=10, skew_factor=0.9):
    data = [(1 if i < num_rows * skew_factor else i % num_partitions + 2, i) for i in range(num_rows)]
    return sc.parallelize(data).partitionBy(num_partitions, SkewedPartitioner(num_partitions))

def get_partition_info(df):
    return df.groupBy(spark_partition_id().alias("partition_id")) \
        .agg(count("*").alias("count")) \
        .orderBy("partition_id")

# Create skewed RDD
spark = SparkSession.builder.getOrCreate()
skewed_rdd = create_skewed_rdd(spark.sparkContext)

# Convert to DataFrame
skewed_df = spark.createDataFrame(skewed_rdd, ["key", "value"])

# Check partition distribution
partition_info = get_partition_info(skewed_df)
partition_info.show()

# Print additional statistics
total_rows = skewed_df.count()
num_partitions = partition_info.count()
max_count = partition_info.agg({"count": "max"}).collect()[0][0]
min_count = partition_info.agg({"count": "min"}).collect()[0][0]

print(f"Total rows: {total_rows}")
print(f"Number of partitions: {num_partitions}")
print(f"Max rows in a partition: {max_count}")
print(f"Min rows in a partition: {min_count}")
print(f"Skew ratio (max/min): {max_count/min_count:.2f}")