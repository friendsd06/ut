from pyspark import RDD, SparkContext
from pyspark.sql import SparkSession

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

# Create skewed RDD
spark = SparkSession.builder.getOrCreate()
skewed_rdd = create_skewed_rdd(spark.sparkContext)

# Convert to DataFrame
skewed_df = spark.createDataFrame(skewed_rdd, ["key", "value"])

# Check partition distribution
get_partition_info(skewed_df).show()