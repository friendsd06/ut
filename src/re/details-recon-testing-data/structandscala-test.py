from pyspark.sql import SparkSession
from pyspark.sql.functions import lit

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("Reconciliation Test Datasets") \
    .getOrCreate()

# Define schemas and create datasets
datasets = []

for i in range(1, 11):
    # Define the data for source and target
    data_src = [
        (1, "Alice", {"city": "NY", "zip": "10001"}, 1000 + i),
        (2, "Bob", {"city": "LA", "zip": "90001"}, 2000 + i),
        (3, "Charlie", {"city": "SF", "zip": "94101"}, 3000 + i),
        (4, "David", {"city": "CHI", "zip": "60601"}, 4000 + i),
        (5, "Eve", {"city": "HOU", "zip": "77001"}, 5000 + i),
    ]

    data_tgt = [
        (1, "Alice", {"city": "NY", "zip": "10001"}, 1000 + i),  # Same as source
        (2, "Bob", {"city": "LA", "zip": "90002"}, 2000 + i),    # Different zip in struct
        (3, "Charlie", {"city": "SF", "zip": "94102"}, 3000 + i),# Different zip in struct
        (4, "David", {"city": "CHI", "zip": "60602"}, 4000 + i), # Different zip in struct
        (5, "Eve", {"city": "HOU", "zip": "77002"}, 5000 + i),   # Different zip in struct
    ]

    # Define schema for the data
    schema = "id INT, name STRING, address STRUCT<city: STRING, zip: STRING>, balance INT"

    # Create source and target DataFrames
    df_src = spark.createDataFrame(data_src, schema=schema)
    df_tgt = spark.createDataFrame(data_tgt, schema=schema)

    # Add the datasets to the list
    datasets.append((f"Dataset {i}", df_src, df_tgt))

# Show the generated datasets
for name, df_src, df_tgt in datasets:
    print(f"\n{name} - Source Data:")
    df_src.show(truncate=False)

    print(f"\n{name} - Target Data:")
    df_tgt.show(truncate=False)
