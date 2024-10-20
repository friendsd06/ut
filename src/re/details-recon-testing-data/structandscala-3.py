from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("Complex Reconciliation Test Datasets") \
    .getOrCreate()

# Define schemas and create datasets with various breaking scenarios
datasets = []

for i in range(1, 11):
    # Define the data for source with intentional type variations and nulls
    data_src = [
        (1, "Alice", {"city": "NY", "zip": "10001"}, 1000 + i),
        (2, "Bob", {"city": "LA", "zip": None}, 2000.5 + i),   # Float instead of integer for balance
        (3, "Charlie", {"city": None, "zip": "94101"}, 3000),  # Null city in struct
        (4, None, {"city": "CHI", "zip": 60601}, 4000 + i),    # Integer zip
        (5, "Eve", {"city": "HOU", "zip": "77001"}, "5000"),   # String balance
    ]

    # Define the data for target with mismatched structures, types, and nulls
    data_tgt = [
        (1, "Alice", {"city": "NY", "zip": 10001}, 1000 + i),  # Integer zip
        (2, "Bob", {"city": "LA", "zip": "90002"}, None),      # Null balance
        (3, "Charlie", {"city": "SF", "zip": None}, 3000 + i), # Null zip in struct
        (4, "David", {"city": 123, "zip": "60601"}, 4000.0),   # Integer city, float balance
        (5, "Eve", None, 5000),                                # Null struct
    ]

    # Define schema for the data with flexibility for various types
    schema = StructType([
        StructField("id", IntegerType(), True),
        StructField("name", StringType(), True),
        StructField("address", StructType([
            StructField("city", StringType(), True),
            StructField("zip", StringType(), True)
        ]), True),
        StructField("balance", StringType(), True)  # Use StringType to accommodate mixed values
    ])

    # Create source and target DataFrames
    df_src = spark.createDataFrame(data_src, schema=schema)
    df_tgt = spark.createDataFrame(data_tgt, schema=schema)

    # Add the datasets to the list
    datasets.append((f"Dataset {i}", df_src, df_tgt))

# Show the generated datasets with various breaking scenarios
for name, df_src, df_tgt in datasets:
    print(f"\n{name} - Source Data:")
    df_src.show(truncate=False)

    print(f"\n{name} - Target Data:")
    df_tgt.show(truncate=False)
