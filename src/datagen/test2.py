from dbldatagen import DataGenerator, seedGenerator
from pyspark.sql.functions import col, min, max, to_timestamp
from pyspark.sql.types import *
from datetime import datetime

# Create sample input data
sample_data = [
    (1, "Product A", "Category 1", datetime(2023, 1, 1), 100, 45.50, "Active"),
    (2, "Product B", "Category 2", datetime(2023, 2, 15), 200, 75.25, "Inactive"),
    (3, "Product C", "Category 1", datetime(2023, 3, 30), 150, 60.75, "Active"),
    (4, "Product D", "Category 3", datetime(2023, 4, 10), 300, 90.00, "Active"),
    (5, "Product E", "Category 2", datetime(2023, 5, 20), 250, 82.50, "Inactive")
]

# Define schema
schema = StructType([
    StructField("id", IntegerType(), False),
    StructField("product_name", StringType(), False),
    StructField("category", StringType(), False),
    StructField("created_date", TimestampType(), False),
    StructField("quantity", IntegerType(), False),
    StructField("price", DoubleType(), False),
    StructField("status", StringType(), False)
])

def create_synthetic_data(input_df, scale_factor=2, seed=42):
    """
    Generate synthetic data based on input dataframe schema
    """
    num_rows = int(input_df.count() * scale_factor)
    schema_fields = input_df.schema.fields

    partitions = max(1, num_rows // 100000)
    dg = (DataGenerator(spark, rows=num_rows, partitions=partitions)
          .withIdOutput()
          .withSeed(seed))

    for field in schema_fields:
        col_name = field.name
        data_type = field.dataType

        try:
            if isinstance(data_type, StringType):
                unique_values = [row[col_name] for row in input_df.select(col_name).distinct().collect()]
                dg = dg.withColumn(col_name, "string", values=unique_values, random=True)

            elif isinstance(data_type, (IntegerType, LongType)):
                range_vals = input_df.agg(min(col_name).alias('min'),
                                          max(col_name).alias('max')).collect()[0]
                dg = dg.withColumn(col_name, "int",
                                   minValue=range_vals.min,
                                   maxValue=range_vals.max,
                                   random=True)

            elif isinstance(data_type, (DoubleType, FloatType)):
                range_vals = input_df.agg(min(col_name).alias('min'),
                                          max(col_name).alias('max')).collect()[0]
                dg = dg.withColumn(col_name, "double",
                                   minValue=float(range_vals.min),
                                   maxValue=float(range_vals.max),
                                   precision=2,
                                   random=True)

            elif isinstance(data_type, TimestampType):
                range_vals = input_df.agg(min(col_name).alias('min'),
                                          max(col_name).alias('max')).collect()[0]
                min_ts = int(range_vals.min.timestamp() * 1000)
                max_ts = int(range_vals.max.timestamp() * 1000)
                dg = dg.withColumn(col_name, "timestamp",
                                   minValue=min_ts,
                                   maxValue=max_ts,
                                   random=True)
        except Exception as e:
            print(f"Error processing column {col_name}: {str(e)}")
            raise

    return dg.build()

# Create input DataFrame
input_df = spark.createDataFrame(sample_data, schema)

# Generate synthetic data with 3x rows
synthetic_df = create_synthetic_data(input_df, scale_factor=3)
synthetic_df.show(truncate=False)