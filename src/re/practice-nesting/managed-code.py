from pyspark.sql import DataFrame
from pyspark.sql.types import StructType, ArrayType, MapType, StructField
from pyspark.sql.functions import explode_outer, col, map_keys, map_values

def flatten_nested_dataframe(nested_df: DataFrame, columns_to_explode: list = None) -> DataFrame:
    """
    Flatten a nested DataFrame by expanding specified nested structures.

    Args:
    nested_df (DataFrame): The input DataFrame with nested structures.
    columns_to_explode (list): Optional list of column names to explode. If None, all nested structures are flattened.

    Returns:
    DataFrame: A new DataFrame with specified nested structures flattened.
    """
    def get_complex_fields(df: DataFrame) -> list:
        """Identify all complex fields (struct, array, map) in the DataFrame."""
        return [
            field for field in df.schema.fields
            if isinstance(field.dataType, (StructType, ArrayType, MapType))
        ]

    def filter_fields_to_process(all_fields: list, columns_to_explode: list) -> list:
        """Filter complex fields based on user-specified columns to explode."""
        if columns_to_explode is None:
            return all_fields
        return [field for field in all_fields if field.name in columns_to_explode]

    def process_struct(df: DataFrame, column_name: str) -> DataFrame:
        """Expand a StructType column into individual columns."""
        struct_fields = df.schema[column_name].dataType.fields
        expanded_cols = [
            col(f"{column_name}.{field.name}").alias(f"{column_name}_{field.name}")
            for field in struct_fields
        ]
        return df.select("*", *expanded_cols).drop(column_name)

    def process_array(df: DataFrame, column_name: str) -> DataFrame:
        """Explode an ArrayType column, creating a new row for each array element."""
        return df.withColumn(f"{column_name}_exploded", explode_outer(column_name)).drop(column_name)

    def process_map(df: DataFrame, column_name: str) -> DataFrame:
        """Split a MapType column into separate columns for keys and values."""
        return df.select(
            "*",
            map_keys(column_name).alias(f"{column_name}_keys"),
            map_values(column_name).alias(f"{column_name}_values")
        ).drop(column_name)

    def process_field(df: DataFrame, field: StructField) -> tuple:
        """Process a single complex field based on its type."""
        if isinstance(field.dataType, StructType):
            return process_struct(df, field.name), None
        elif isinstance(field.dataType, ArrayType):
            processed_df = process_array(df, field.name)
            if isinstance(field.dataType.elementType, (StructType, ArrayType, MapType)):
                return processed_df, StructField(f"{field.name}_exploded", field.dataType.elementType)
            return processed_df, None
        elif isinstance(field.dataType, MapType):
            return process_map(df, field.name), None
        return df, None

    def update_fields_to_process(all_fields: list, columns_to_explode: list) -> list:
        """Update the list of fields to process after each transformation."""
        if columns_to_explode is None:
            return all_fields
        return [
            field for field in all_fields
            if field.name in columns_to_explode or
               any(field.name.startswith(f"{col}_") for col in columns_to_explode)
        ]

    # Main flattening logic
    complex_fields = get_complex_fields(nested_df)
    fields_to_process = filter_fields_to_process(complex_fields, columns_to_explode)

    while fields_to_process:
        current_field = fields_to_process.pop(0)
        nested_df, additional_field = process_field(nested_df, current_field)

        if additional_field:
            fields_to_process.append(additional_field)

        complex_fields = get_complex_fields(nested_df)
        fields_to_process = update_fields_to_process(complex_fields, columns_to_explode)

    return nested_df


    # Scenario 1: Simple Struct
print("Scenario 1: Simple Struct")
schema1 = StructType([
    StructField("id", IntegerType(), True),
    StructField("info", StructType([
        StructField("name", StringType(), True),
        StructField("age", IntegerType(), True)
    ]), True)
])
data1 = [(1, ("Alice", 30)), (2, ("Bob", 25))]
df1 = spark.createDataFrame(data1, schema1)

print("Original DataFrame:")
df1.show()
df1.printSchema()

flattened_df1 = flatten_nested_dataframe(df1)
print("Flattened DataFrame:")
flattened_df1.show()
flattened_df1.printSchema()

# Scenario 2: Array of Primitives
print("\nScenario 2: Array of Primitives")
schema2 = StructType([
    StructField("id", IntegerType(), True),
    StructField("tags", ArrayType(StringType()), True)
])
data2 = [(1, ["red", "blue"]), (2, ["green", "yellow"]), (3, ["orange"])]
df2 = spark.createDataFrame(data2, schema2)

print("Original DataFrame:")
df2.show()
df2.printSchema()

flattened_df2 = flatten_nested_dataframe(df2)
print("Flattened DataFrame:")
flattened_df2.show()
flattened_df2.printSchema()

# Scenario 3: Nested Struct with Array
print("\nScenario 3: Nested Struct with Array")
schema3 = StructType([
    StructField("id", IntegerType(), True),
    StructField("user", StructType([
        StructField("name", StringType(), True),
        StructField("scores", ArrayType(IntegerType()), True)
    ]), True)
])
data3 = [(1, ("Alice", [85, 90, 95])), (2, ("Bob", [70, 75, 80]))]
df3 = spark.createDataFrame(data3, schema3)

print("Original DataFrame:")
df3.show()
df3.printSchema()

flattened_df3 = flatten_nested_dataframe(df3)
print("Flattened DataFrame:")
flattened_df3.show()
flattened_df3.printSchema()

# Scenario 4: Complex Nested Structure
print("\nScenario 4: Complex Nested Structure")
schema4 = StructType([
    StructField("id", IntegerType(), True),
    StructField("user", StructType([
        StructField("name", StringType(), True),
        StructField("address", StructType([
            StructField("city", StringType(), True),
            StructField("zip", StringType(), True)
        ]), True),
        StructField("orders", ArrayType(StructType([
            StructField("order_id", IntegerType(), True),
            StructField("amount", DoubleType(), True)
        ])), True)
    ]), True)
])
data4 = [
    (1, ("Alice", ("New York", "10001"), [(101, 99.99), (102, 49.99)])),
    (2, ("Bob", ("Los Angeles", "90001"), [(103, 149.99)]))
]
df4 = spark.createDataFrame(data4, schema4)

print("Original DataFrame:")
df4.show(truncate=False)
df4.printSchema()

flattened_df4 = flatten_nested_dataframe(df4)
print("Flattened DataFrame:")
flattened_df4.show(truncate=False)
flattened_df4.printSchema()

# Scenario 5: Mixed Types with Map and Selective Flattening
print("\nScenario 5: Mixed Types with Map and Selective Flattening")
schema5 = StructType([
    StructField("id", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("details", StructType([
        StructField("age", IntegerType(), True),
        StructField("hobbies", ArrayType(StringType()), True)
    ]), True),
    StructField("scores", MapType(StringType(), IntegerType()), True)
])
data5 = [
    (1, "Alice", (25, ["reading", "swimming"]), {"math": 90, "science": 95}),
    (2, "Bob", (30, ["cycling", "photography"]), {"math": 85, "science": 88})
]
df5 = spark.createDataFrame(data5, schema5)

print("Original DataFrame:")
df5.show(truncate=False)
df5.printSchema()

# Flatten only 'details' and 'scores'
flattened_df5 = flatten_nested_dataframe(df5, columns_to_explode=['details', 'scores'])
print("Selectively Flattened DataFrame:")
flattened_df5.show(truncate=False)
flattened_df5.printSchema()
# Example usage:
# spark = SparkSession.builder.appName("NestedDataFrameFlattening").getOrCreate()
#
# # Create your nested_df here
#
# # Flatten all nested structures
# flattened_df_all = flatten_nested_dataframe(nested_df)
#
# # Flatten only specific columns
# flattened_df_specific = flatten_nested_dataframe(nested_df, columns_to_explode=['purchase_history', 'customer_address'])
#
# # Display results
# flattened_df_all.show(truncate=False)
# flattened_df_all.printSchema()
#
# flattened_df_specific.show(truncate=False)
# flattened_df_specific.printSchema()
#
# spark.stop()