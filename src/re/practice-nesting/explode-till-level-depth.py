from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, ArrayType, MapType, DoubleType
from pyspark.sql.functions import explode_outer, col, map_keys, map_values, struct, array, create_map, lit
from typing import Dict, Optional

def flatten_nested_dataframe(nested_df: DataFrame, columns_to_explode: Optional[Dict[str, int]] = None) -> DataFrame:
    """
    Flatten a nested DataFrame by expanding specified nested structures to a specified depth.

    Args:
    nested_df (DataFrame): The input DataFrame with nested structures.
    columns_to_explode (Dict[str, int]): Optional dictionary of column names to explode and their max depth.
                                         If None, all nested structures are flattened completely.
                                         Use depth 0 for no flattening, 1 for one level, etc.

    Returns:
    DataFrame: A new DataFrame with specified nested structures flattened.
    """
    def get_complex_fields(df: DataFrame) -> list:
        """Identify all complex fields (struct, array, map) in the DataFrame."""
        return [
            field for field in df.schema.fields
            if isinstance(field.dataType, (StructType, ArrayType, MapType))
        ]

    def should_process_field(field_name: str, current_depth: int) -> bool:
        """Determine if a field should be processed based on the specified depth."""
        if columns_to_explode is None:
            return True
        parts = field_name.split('_')
        for i in range(len(parts)):
            key = '_'.join(parts[:i+1])
            if key in columns_to_explode:
                return current_depth < columns_to_explode[key]
        return False

    def process_struct(df: DataFrame, column_name: str, current_depth: int) -> DataFrame:
        """Expand a StructType column into individual columns."""
        struct_fields = df.schema[column_name].dataType.fields
        expanded_cols = [
            col(f"{column_name}.{field.name}").alias(f"{column_name}_{field.name}")
            for field in struct_fields
            if should_process_field(f"{column_name}_{field.name}", current_depth + 1)
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

    def process_field(df: DataFrame, field: StructField, current_depth: int) -> tuple:
        """Process a single complex field based on its type and current depth."""
        if not should_process_field(field.name, current_depth):
            return df, None

        if isinstance(field.dataType, StructType):
            return process_struct(df, field.name, current_depth), None
        elif isinstance(field.dataType, ArrayType):
            processed_df = process_array(df, field.name)
            if isinstance(field.dataType.elementType, (StructType, ArrayType, MapType)):
                return processed_df, StructField(f"{field.name}_exploded", field.dataType.elementType)
            return processed_df, None
        elif isinstance(field.dataType, MapType):
            return process_map(df, field.name), None
        return df, None

    def flatten_recursive(df: DataFrame, current_depth: int = 0) -> DataFrame:
        """Recursively flatten the DataFrame to the specified depth."""
        complex_fields = get_complex_fields(df)

        for field in complex_fields:
            df, additional_field = process_field(df, field, current_depth)
            if additional_field:
                df = flatten_recursive(df, current_depth + 1)

        return df

    return flatten_recursive(nested_df)

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
    StructField("preferences", MapType(StringType(), StringType()))
])

# Create sample data
data = [
    (1, "Alice",
     {"street": "123 Main St", "city": "New York", "geo": {"lat": 40.7128, "lon": -74.0060}},
     [{"order_id": 101, "product": "Laptop", "quantity": 1, "details": {"color": "silver", "brand": "TechCo"}},
      {"order_id": 102, "product": "Mouse", "quantity": 2, "details": {"color": "black", "brand": "ClickMaster"}}],
     {"newsletter": "weekly", "theme": "dark"}
     ),
    (2, "Bob",
     {"street": "456 Elm St", "city": "San Francisco", "geo": {"lat": 37.7749, "lon": -122.4194}},
     [{"order_id": 103, "product": "Keyboard", "quantity": 1, "details": {"color": "white", "brand": "TypePro"}}],
     {"newsletter": "daily", "theme": "light"}
     )
]

# Create the nested DataFrame
nested_df = spark.createDataFrame(data, schema)

print("Original Nested DataFrame Schema:")
nested_df.printSchema()

# Test Case 1: Flatten all nested structures
flattened_df_all = flatten_nested_dataframe(nested_df)
print("\nFully Flattened DataFrame Schema:")
flattened_df_all.printSchema()

# Test Case 2: Selective flattening with depth control
columns_to_explode = {
    'address': 1,       # Flatten address one level
    'orders': 1,        # Flatten orders one level
    'preferences': 0    # Don't flatten preferences
}
flattened_df_selective = flatten_nested_dataframe(nested_df, columns_to_explode)
print("\nSelectively Flattened DataFrame Schema:")
flattened_df_selective.printSchema()

# Display sample data
print("\nSample Data from Selectively Flattened DataFrame:")
flattened_df_selective.show(truncate=False)

# Clean up
spark.stop()