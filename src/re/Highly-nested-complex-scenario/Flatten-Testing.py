# Import necessary libraries
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, explode_outer, sort_array
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, ArrayType, DoubleType

# Initialize SparkSession
spark = SparkSession.builder \
    .appName("FlattenNestedDataFrames") \
    .master("local[*]") \
    .getOrCreate()

def flatten_df(df: DataFrame) -> DataFrame:
    """
    Recursively flattens a Spark DataFrame with nested structures (structs and arrays).

    Args:
        df (DataFrame): The input Spark DataFrame with nested columns.

    Returns:
        DataFrame: A flattened Spark DataFrame.
    """
    # Initialize lists to keep track of fields to process
    flat_cols = []
    array_struct_cols = []
    array_scalar_cols = []

    # Iterate through all fields in the DataFrame's schema
    for field in df.schema.fields:
        field_name = field.name
        field_type = field.dataType

        if isinstance(field_type, StructType):
            # If the field is a struct, flatten its fields
            for subfield in field_type.fields:
                flat_cols.append(col(f"{field_name}.{subfield.name}").alias(f"{field_name}_{subfield.name}"))
        elif isinstance(field_type, ArrayType):
            element_type = field_type.elementType
            if isinstance(element_type, StructType):
                # If the array contains structs, mark it for exploding
                array_struct_cols.append(field_name)
            else:
                # If the array contains scalars, sort it
                array_scalar_cols.append(field_name)
                flat_cols.append(col(field_name))
        else:
            # If the field is neither a struct nor an array, keep it as is
            flat_cols.append(col(field_name))

    # Explode array columns containing structs
    for array_col in array_struct_cols:
        df = df.withColumn(array_col, explode_outer(col(array_col)))
        # After exploding, flatten the struct fields
        for subfield in df.schema[array_col].dataType.elementType.fields:
            df = df.withColumn(f"{array_col}_{subfield.name}", col(f"{array_col}.{subfield.name}"))
        # Drop the original exploded array column
        df = df.drop(array_col)

    # Sort array columns containing scalars in ascending order
    for array_col in array_scalar_cols:
        df = df.withColumn(array_col, sort_array(col(array_col), ascending=True))

    # Select all flattened columns
    df_flat = df.select(*flat_cols)

    # Check if there are still nested columns; if so, recursively flatten
    if any(isinstance(field.dataType, StructType) or isinstance(field.dataType, ArrayType) for field in df_flat.schema.fields):
        return flatten_df(df_flat)
    else:
        return df_flat

# Create Sample Data for Table 1
data1 = [
    (
        1,
        "John",
        {"age": 30, "city": "New York"},
        [
            {"type": "home", "number": "123-456-7890"},
            {"type": "work", "number": "098-765-4321"}
        ],
        {"scores": [85, 90, 78], "average": 84.3}
    ),
    (
        2,
        "Alice",
        {"age": 25, "city": "San Francisco"},
        [
            {"type": "home", "number": "111-222-3333"}
        ],
        {"scores": [92, 88, 95], "average": 91.7}
    ),
    (
        3,
        "Bob",
        {"age": 35, "city": "Chicago"},
        [],
        {"scores": [75, 80, 82], "average": 79.0}
    )
]

schema1 = StructType([
    StructField("id", IntegerType(), False),
    StructField("name", StringType(), False),
    StructField("info", StructType([
        StructField("age", IntegerType(), True),
        StructField("city", StringType(), True)
    ]), True),
    StructField("phones", ArrayType(StructType([
        StructField("type", StringType(), True),
        StructField("number", StringType(), True)
    ])), True),
    StructField("grades", StructType([
        StructField("scores", ArrayType(IntegerType()), True),
        StructField("average", DoubleType(), True)
    ]), True)
])

# Create DataFrame for Table 1
df1 = spark.createDataFrame(data1, schema1)

# Create Sample Data for Table 2 (Updated Data)
data2 = [
    (
        1,
        "John",
        {"age": 31, "city": "New York"},  # Age changed
        [
            {"type": "home", "number": "123-456-7890"},
            {"type": "work", "number": "098-765-4321"}
        ],
        {"scores": [85, 90, 78], "average": 84.3}
    ),
    (
        2,
        "Alice",
        {"age": 25, "city": "San Francisco"},
        [
            {"type": "home", "number": "111-222-3333"},
            {"type": "mobile", "number": "222-333-4444"}  # New phone added
        ],
        {"scores": [92, 88, 95, 100], "average": 93.8}  # New score added
    ),
    (
        4,
        "Charlie",
        {"age": 28, "city": "Boston"},
        [
            {"type": "home", "number": "555-666-7777"}
        ],
        {"scores": [80, 85, 88], "average": 84.3}  # New record
    )
]

schema2 = StructType([
    StructField("id", IntegerType(), False),
    StructField("name", StringType(), False),
    StructField("info", StructType([
        StructField("age", IntegerType(), True),
        StructField("city", StringType(), True)
    ]), True),
    StructField("phones", ArrayType(StructType([
        StructField("type", StringType(), True),
        StructField("number", StringType(), True)
    ])), True),
    StructField("grades", StructType([
        StructField("scores", ArrayType(IntegerType()), True),
        StructField("average", DoubleType(), True)
    ]), True)
])

# Create DataFrame for Table 2
df2 = spark.createDataFrame(data2, schema2)

# Display Original DataFrames
print("Original DataFrame - Table 1:")
df1.show(truncate=False)
df1.printSchema()

print("\nOriginal DataFrame - Table 2:")
df2.show(truncate=False)
df2.printSchema()

# Apply the flattening function to both DataFrames
df1_flat = flatten_df(df1)
df2_flat = flatten_df(df2)

# Display Flattened DataFrames
print("\nFlattened DataFrame - Table 1:")
df1_flat.show(truncate=False)
df1_flat.printSchema()

print("\nFlattened DataFrame - Table 2:")
df2_flat.show(truncate=False)
df2_flat.printSchema()

# Stop the SparkSession
spark.stop()
