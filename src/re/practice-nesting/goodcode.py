from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, ArrayType, DoubleType, MapType
from pyspark.sql.functions import explode_outer, col, map_keys, map_values

# Initialize Spark Session
spark = SparkSession.builder.appName("HighlyReadableNestedFlatten").getOrCreate()

# Define Schema with nested structures
customer_schema = StructType([
    StructField("customer_id", IntegerType(), True),
    StructField("customer_name", StringType(), True),
    StructField("customer_address", StructType([
        StructField("street", StringType(), True),
        StructField("city", StringType(), True),
        StructField("location", StructType([
            StructField("latitude", DoubleType(), True),
            StructField("longitude", DoubleType(), True)
        ]), True)
    ]), True),
    StructField("purchase_history", ArrayType(StructType([
        StructField("order_id", IntegerType(), True),
        StructField("product_name", StringType(), True),
        StructField("purchase_amount", DoubleType(), True)
    ])), True),
    StructField("customer_tags", ArrayType(StringType(), True), True),
    StructField("additional_info", MapType(StringType(), StringType(), True), True)
])

# Sample customer data
customer_data = [
    (1, "Alice Johnson",
     {"street": "123 Maple St", "city": "Springfield", "location": {"latitude": 44.123, "longitude": -122.456}},
     [{"order_id": 101, "product_name": "Laptop", "purchase_amount": 999.99},
      {"order_id": 102, "product_name": "Mouse", "purchase_amount": 24.99}],
     ["loyal", "tech-savvy"],
     {"account_type": "premium", "last_login": "2023-10-15"}
     ),
    (2, "Bob Smith",
     {"street": "456 Oak Ave", "city": "Shelbyville", "location": {"latitude": 44.789, "longitude": -122.987}},
     [{"order_id": 103, "product_name": "Headphones", "purchase_amount": 79.99}],
     ["new-customer"],
     {"account_type": "standard", "last_login": "2023-10-16"}
     )
]

# Create DataFrame with nested structures
nested_df = spark.createDataFrame(customer_data, customer_schema)

def flatten_nested_dataframe(nested_df):
    """
    Flatten a nested DataFrame by expanding all nested structures (StructType, ArrayType, MapType).

    Args:
    nested_df (DataFrame): The input DataFrame with nested structures.

    Returns:
    DataFrame: A new DataFrame with all nested structures flattened.
    """

    def expand_struct(df, struct_column_name):
        """Expand a StructType column into individual columns."""
        struct_fields = df.schema[struct_column_name].dataType.fields
        expanded_cols = [col(f"{struct_column_name}.{field.name}").alias(f"{struct_column_name}_{field.name}")
                         for field in struct_fields]
        return df.select("*", *expanded_cols).drop(struct_column_name)

    def explode_array(df, array_column_name):
        """Explode an ArrayType column, creating a new row for each array element."""
        return df.withColumn(f"{array_column_name}_exploded", explode_outer(array_column_name)).drop(array_column_name)

    def split_map(df, map_column_name):
        """Split a MapType column into separate columns for keys and values."""
        return df.select("*",
                         map_keys(map_column_name).alias(f"{map_column_name}_keys"),
                         map_values(map_column_name).alias(f"{map_column_name}_values")).drop(map_column_name)

    # Identify all complex fields (struct, array, map) in the DataFrame
    complex_fields_to_process = [
        field for field in nested_df.schema.fields
        if isinstance(field.dataType, (StructType, ArrayType, MapType))
    ]

    # Continue processing until all complex fields are flattened
    while complex_fields_to_process:
        current_field = complex_fields_to_process.pop(0)

        if isinstance(current_field.dataType, StructType):
            nested_df = expand_struct(nested_df, current_field.name)
        elif isinstance(current_field.dataType, ArrayType):
            nested_df = explode_array(nested_df, current_field.name)
            # If the array contains complex types, add it back to the list for further processing
            if isinstance(current_field.dataType.elementType, (StructType, ArrayType, MapType)):
                complex_fields_to_process.append(
                    StructField(f"{current_field.name}_exploded", current_field.dataType.elementType)
                )
        elif isinstance(current_field.dataType, MapType):
            nested_df = split_map(nested_df, current_field.name)

        # Update the list of complex fields after each transformation
        complex_fields_to_process = [
            field for field in nested_df.schema.fields
            if isinstance(field.dataType, (StructType, ArrayType, MapType))
        ]

    return nested_df

# Apply the flattening function
flattened_df = flatten_nested_dataframe(nested_df)

# Display results
print("Original Nested DataFrame:")
nested_df.show(truncate=False)
nested_df.printSchema()

print("\nFlattened DataFrame:")
flattened_df.show(truncate=False)
flattened_df.printSchema()

# Stop Spark Session
spark.stop()