from dbldatagen import DataGenerator
from pyspark.sql.functions import col, min, max
from pyspark.sql.types import *
from datetime import datetime

# Sample data
sample_data = [
    (1, "Product A", "Category 1", datetime(2023, 1, 1), 100, 45.50, "Active"),
    (2, "Product B", "Category 2", datetime(2023, 2, 15), 200, 75.25, "Inactive"),
    (3, "Product C", "Category 1", datetime(2023, 3, 30), 150, 60.75, "Active"),
    (4, "Product D", "Category 3", datetime(2023, 4, 10), 300, 90.00, "Active"),
    (5, "Product E", "Category 2", datetime(2023, 5, 20), 250, 82.50, "Inactive")
]

schema = StructType([
    StructField("id", IntegerType(), False),
    StructField("product_name", StringType(), False),
    StructField("category", StringType(), False),
    StructField("created_date", TimestampType(), False),
    StructField("quantity", IntegerType(), False),
    StructField("price", DoubleType(), False),
    StructField("status", StringType(), False)
])

def create_synthetic_data(input_df):
    """
    Generate synthetic data based on input dataframe schema
    Fixed parameters: rows=1000000, partitions=8
    """
    dg = DataGenerator(spark, rows=1000000, partitions=8).withIdOutput()
    input_df.cache()

    for field in input_df.schema.fields:
        col_name = field.name
        data_type = field.dataType

        try:
            if isinstance(data_type, StringType):
                distinct_values = (input_df.select(col_name)
                                   .distinct()
                                   .limit(1000)
                                   .rdd.map(lambda x: x[0])
                                   .toLocalIterator())
                unique_values = [val for val in distinct_values]
                dg = dg.withColumn(col_name, "string", values=unique_values, random=True)

            elif isinstance(data_type, (IntegerType, LongType)):
                range_vals = input_df.select(
                    min(col(col_name)).alias("min"),
                    max(col(col_name)).alias("max")
                ).first()
                dg = dg.withColumn(col_name, "int",
                                   minValue=range_vals["min"],
                                   maxValue=range_vals["max"],
                                   random=True)

            elif isinstance(data_type, (DoubleType, FloatType)):
                range_vals = input_df.select(
                    min(col(col_name)).alias("min"),
                    max(col(col_name)).alias("max")
                ).first()
                dg = dg.withColumn(col_name, "double",
                                   minValue=float(range_vals["min"]),
                                   maxValue=float(range_vals["max"]),
                                   precision=2,
                                   random=True)

            elif isinstance(data_type, TimestampType):
                range_vals = input_df.select(
                    min(col(col_name)).alias("min"),
                    max(col(col_name)).alias("max")
                ).first()
                min_ts = int(range_vals["min"].timestamp() * 1000)
                max_ts = int(range_vals["max"].timestamp() * 1000)
                dg = dg.withColumn(col_name, "timestamp",
                                   minValue=min_ts,
                                   maxValue=max_ts,
                                   random=True)

        except Exception as e:
            print(f"Error processing column {col_name}: {str(e)}")
            raise

    input_df.unpersist()
    return dg.build()

# Usage
input_df = spark.createDataFrame(sample_data, schema)
synthetic_df = create_synthetic_data(input_df)
synthetic_df.show(truncate=False)