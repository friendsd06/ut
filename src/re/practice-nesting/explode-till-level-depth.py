from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, ArrayType, MapType, DoubleType
from pyspark.sql.functions import col, explode_outer, map_keys, map_values
from typing import Dict, Optional, List

def flatten_nested_dataframe(df: DataFrame, max_depth: Optional[Dict[str, int]] = None) -> DataFrame:
    """
    Flatten a nested DataFrame by expanding specified nested structures to a specified depth.

    Args:
    df (DataFrame): The input DataFrame with nested structures.
    max_depth (Dict[str, int]): Optional dictionary of column prefixes and their max depth to flatten.
                                If None, all nested structures are flattened completely.
                                Use depth 0 for no flattening, 1 for one level, etc.

    Returns:
    DataFrame: A new DataFrame with specified nested structures flattened.
    """
    def should_process(field_name: str, current_depth: int) -> bool:
        if max_depth is None:
            return True
        for prefix, depth in max_depth.items():
            if field_name.startswith(prefix) and current_depth < depth:
                return True
        return False

    def flatten_schema(schema: StructType, prefix: str = "", current_depth: int = 0) -> List[str]:
        fields = []
        for field in schema.fields:
            field_name = f"{prefix}{field.name}"
            if isinstance(field.dataType, StructType) and should_process(field_name, current_depth):
                fields.extend(flatten_schema(field.dataType, f"{field_name}_", current_depth + 1))
            elif isinstance(field.dataType, ArrayType):
                if should_process(field_name, current_depth):
                    if isinstance(field.dataType.elementType, StructType):
                        fields.append(f"{field_name}_exploded")
                        fields.extend(flatten_schema(field.dataType.elementType, f"{field_name}_", current_depth + 1))
                    else:
                        fields.append(f"{field_name}_exploded")
                else:
                    fields.append(field_name)
            elif isinstance(field.dataType, MapType) and should_process(field_name, current_depth):
                fields.append(f"{field_name}_key")
                fields.append(f"{field_name}_value")
            else:
                fields.append(field_name)
        return fields

    def flatten_dataframe(df: DataFrame, prefix: str = "", current_depth: int = 0) -> DataFrame:
        for field in df.schema.fields:
            field_name = f"{prefix}{field.name}"
            if isinstance(field.dataType, StructType) and should_process(field_name, current_depth):
                df = df.select("*", *[col(f"{field_name}.{subfield.name}").alias(f"{field_name}_{subfield.name}") for subfield in field.dataType.fields]).drop(field_name)
                df = flatten_dataframe(df, f"{field_name}_", current_depth + 1)
            elif isinstance(field.dataType, ArrayType) and should_process(field_name, current_depth):
                df = df.withColumn(f"{field_name}_exploded", explode_outer(field_name)).drop(field_name)
                if isinstance(field.dataType.elementType, StructType):
                    for subfield in field.dataType.elementType.fields:
                        df = df.withColumn(f"{field_name}_{subfield.name}", col(f"{field_name}_exploded.{subfield.name}"))
                    df = df.drop(f"{field_name}_exploded")
                    df = flatten_dataframe(df, f"{field_name}_", current_depth + 1)
            elif isinstance(field.dataType, MapType) and should_process(field_name, current_depth):
                df = df.select("*", map_keys(field_name).alias(f"{field_name}_key"), map_values(field_name).alias(f"{field_name}_value")).drop(field_name)
        return df

    flattened_df = flatten_dataframe(df)
    flattened_fields = flatten_schema(df.schema)
    return flattened_df.select(*flattened_fields)

# Create a SparkSession
spark = SparkSession.builder.appName("NestedDataFrameFlattening").getOrCreate()

# Define a complex nested schema
schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("address", StructType([
        StructField("street", StringType(), True),
        StructField("city", StringType(), True),
        StructField("geo", StructType([
            StructField("lat", DoubleType(), True),
            StructField("lon", DoubleType(), True)
        ]))
    ])),
    StructField("orders", ArrayType(StructType([
        StructField("order_id", IntegerType(), True),
        StructField("product", StringType(), True),
        StructField("quantity", IntegerType(), True),
        StructField("details", MapType(StringType(), StringType()))
    ]))),
    StructField("tags", ArrayType(StringType(), True)),
    StructField("preferences", MapType(StringType(), StringType())),
    StructField("nested_array", ArrayType(ArrayType(IntegerType())))
])

# Create sample data
data = [
    (1, "Alice",
     {"street": "123 Main St", "city": "New York", "geo": {"lat": 40.7128, "lon": -74.0060}},
     [{"order_id": 101, "product": "Laptop", "quantity": 1, "details": {"color": "silver", "brand": "TechCo"}},
      {"order_id": 102, "product": "Mouse", "quantity": 2, "details": {"color": "black", "brand": "ClickMaster"}}],
     ["customer", "vip"],
     {"newsletter": "weekly", "theme": "dark"},
     [[1, 2], [3, 4]]
     ),
    (2, "Bob",
     {"street": "456 Elm St", "city": "San Francisco", "geo": {"lat": 37.7749, "lon": -122.4194}},
     [{"order_id": 103, "product": "Keyboard", "quantity": 1, "details": {"color": "white", "brand": "TypePro"}}],
     ["new_customer"],
     {"newsletter": "daily", "theme": "light"},
     [[5, 6]]
     )
]

# Create the nested DataFrame
nested_df = spark.createDataFrame(data, schema)

print("Original Nested DataFrame Schema:")
nested_df.printSchema()
nested_df.show(truncate=False)

# Test Case 1: Flatten all nested structures
flattened_df_all = flatten_nested_dataframe(nested_df)
print("\nFully Flattened DataFrame Schema:")
flattened_df_all.printSchema()
flattened_df_all.show(truncate=False)

# Test Case 2: Selective flattening with depth control
max_depth = {
    'address': 1,       # Flatten address one level
    'orders': 1,        # Flatten orders one level
    'preferences': 0,   # Don't flatten preferences
    'nested_array': 1   # Flatten nested_array one level
}
flattened_df_selective = flatten_nested_dataframe(nested_df, max_depth)
print("\nSelectively Flattened DataFrame Schema:")
flattened_df_selective.printSchema()
flattened_df_selective.show(truncate=False)

# Clean up
spark.stop()