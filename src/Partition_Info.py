from pyspark.sql.functions import spark_partition_id, count

def get_partition_info(df):
    return df.groupBy(spark_partition_id().alias("partition_id")) \
        .agg(count("*").alias("count")) \
        .orderBy("partition_id")

# Usage
partition_info = get_partition_info(your_dataframe)
partition_info.show()