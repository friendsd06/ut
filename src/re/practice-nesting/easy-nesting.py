from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, ArrayType, DoubleType
from pyspark.sql.functions import explode_outer, col

# Initialize Spark Session
spark = SparkSession.builder.appName("NestedFlattenDataFrame").getOrCreate()

# Define Schema (with more nesting)
schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("address", StructType([
        StructField("street", StringType(), True),
        StructField("city", StringType(), True),
        StructField("geo", StructType([
            StructField("lat", DoubleType(), True),
            StructField("lon", DoubleType(), True)
        ]), True)
    ]), True),
    StructField("orders", ArrayType(StructType([
        StructField("order_id", IntegerType(), True),
        StructField("product", StringType(), True),
        StructField("amount", DoubleType(), True),
        StructField("details", StructType([
            StructField("category", StringType(), True),
            StructField("weight", DoubleType(), True)
        ]), True)
    ])), True)
])

# Sample Data (with more nesting)
data = [
    (1, "Alice", {"street": "123 Maple St", "city": "Springfield", "geo": {"lat": 44.123, "lon": -122.456}},
     [{"order_id": 101, "product": "Book", "amount": 12.99, "details": {"category": "Fiction", "weight": 0.5}},
      {"order_id": 102, "product": "Pen", "amount": 1.99, "details": {"category": "Stationery", "weight": 0.05}}]),
    (2, "Bob", {"street": "456 Oak Ave", "city": "Shelbyville", "geo": {"lat": 44.789, "lon": -122.987}},
     [{"order_id": 103, "product": "Notebook", "amount": 5.49, "details": {"category": "Stationery", "weight": 0.3}}])
]

# Create DataFrame
df = spark.createDataFrame(data, schema)

def flatten_df(df):
    fields = df.schema.fields
    flat_cols = [col(c.name) for c in fields if not isinstance(c.dataType, (ArrayType, StructType))]
    nested_cols = [c.name for c in fields if isinstance(c.dataType, (ArrayType, StructType))]

    while len(nested_cols) > 0:
        col_name = nested_cols.pop(0)
        col_type = [c for c in fields if c.name == col_name][0].dataType

        if isinstance(col_type, StructType):
            expanded = [col(f"{col_name}.{k}").alias(f"{col_name}_{k}") for k in col_type.fieldNames()]
            flat_cols.extend(expanded)
            fields = fields + [StructField(f"{col_name}_{k}", v.dataType) for k, v in col_type.fields]
        elif isinstance(col_type, ArrayType):
            df = df.withColumn(col_name, explode_outer(col_name))
            fields = [StructField(col_name, col_type.elementType)] + fields
            nested_cols.append(col_name)

    df = df.select(flat_cols)
    return df

# Apply Flatten Function
flattened_df = flatten_df(df)

# Show results
print("Original DataFrame:")
df.show(truncate=False)
df.printSchema()

print("\nFlattened DataFrame:")
flattened_df.show(truncate=False)
flattened_df.printSchema()

# Stop Spark Session
spark.stop()