from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("Reconciliation Test Datasets with Nulls") \
    .getOrCreate()

# Define schemas and create datasets with null values
datasets = []

for i in range(1, 11):
    # Define the data for source and target
    data_src = [
        (1, "Alice", {"city": "NY", "zip": "10001"}, 1000 + i),
        (2, "Bob", {"city": "LA", "zip": None}, 2000 + i),      # Null zip in struct
        (3, "Charlie", None, 3000 + i),                        # Null struct
        (4, None, {"city": "CHI", "zip": "60601"}, 4000 + i),  # Null name
        (5, "Eve", {"city": None, "zip": "77001"}, None),      # Null city in struct & balance
    ]

    data_tgt = [
        (1, "Alice", {"city": "NY", "zip": "10001"}, 1000 + i),    # Same as source
        (2, "Bob", {"city": "LA", "zip": "90002"}, None),          # Null balance
        (3, None, {"city": "SF", "zip": "94102"}, 3000 + i),       # Null name in target
        (4, "David", {"city": "CHI", "zip": None}, 4000 + i),      # Null zip in struct
        (5, "Eve", {"city": "HOU", "zip": "77002"}, 5000 + i),     # Different city in struct
    ]

    # Define schema for the data
    schema = StructType([
        StructField("id", IntegerType(), True),
        StructField("name", StringType(), True),
        StructField("address", StructType([
            StructField("city", StringType(), True),
            StructField("zip", StringType(), True)
        ]), True),
        StructField("balance", IntegerType(), True)
    ])

    # Create source and target DataFrames
    df_src = spark.createDataFrame(data_src, schema=schema)
    df_tgt = spark.createDataFrame(data_tgt, schema=schema)

    # Add the datasets to the list
    datasets.append((f"Dataset {i}", df_src, df_tgt))

# Show the generated datasets with nulls
for name, df_src, df_tgt in datasets:
    print(f"\n{name} - Source Data:")
    df_src.show(truncate=False)

    print(f"\n{name} - Target Data:")
    df_tgt.show(truncate=False)
