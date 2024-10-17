from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, ArrayType,
    MapType, DoubleType, DateType
)
from pyspark.sql.functions import explode_outer, col, map_keys, map_values, when, concat_ws, concat
from functools import reduce
import logging

def flatten_nested_dataframe(
        nested_df: DataFrame,
        columns_to_explode: list = None,
        max_depth: int = None
) -> DataFrame:
    """
    Flatten a nested DataFrame by expanding specified nested structures up to a given depth.

    Args:
        nested_df (DataFrame): The input DataFrame with nested structures.
        columns_to_explode (list): Optional list of column names to explode. If None, all nested structures are flattened.
        max_depth (int): The maximum depth to flatten nested structures. If None, flatten all levels.

    Returns:
        DataFrame: A new DataFrame with specified nested structures flattened up to the given depth.
    """

    # Initialize logging
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)

    # Input validation for max_depth
    if max_depth is not None:
        if not isinstance(max_depth, int) or max_depth < 1:
            raise ValueError("max_depth must be a positive integer or None.")

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
        logger.info(f"Flattening Struct field: {column_name}")
        return df.select("*", *expanded_cols).drop(column_name)

    def process_array(df: DataFrame, column_name: str) -> DataFrame:
        """Explode an ArrayType column, creating a new row for each array element."""
        logger.info(f"Exploding Array field: {column_name}")
        return df.withColumn(f"{column_name}_exploded", explode_outer(column_name)).drop(column_name)

    def process_map(df: DataFrame, column_name: str) -> DataFrame:
        """Split a MapType column into separate columns for keys and values."""
        logger.info(f"Processing Map field: {column_name}")
        return df.select(
            "*",
            map_keys(column_name).alias(f"{column_name}_keys"),
            map_values(column_name).alias(f"{column_name}_values")
        ).drop(column_name)

    def process_field(df: DataFrame, field: StructField, current_depth: int, max_depth: int) -> tuple:
        """Process a single complex field based on its type."""
        if max_depth is not None and current_depth >= max_depth:
            # Do not process further if max_depth is reached
            logger.info(f"Max depth {max_depth} reached. Skipping field: {field.name}")
            return df, None
        if isinstance(field.dataType, StructType):
            return process_struct(df, field.name), None
        elif isinstance(field.dataType, ArrayType):
            processed_df = process_array(df, field.name)
            element_type = field.dataType.elementType
            if isinstance(element_type, (StructType, ArrayType, MapType)):
                # If the array elements are complex types, return the new field to process
                return processed_df, StructField(f"{field.name}_exploded", element_type)
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

    # Initialize depth
    current_depth = 0

    while fields_to_process:
        if max_depth is not None and current_depth >= max_depth:
            logger.info(f"Reached max_depth of {max_depth}. Stopping further flattening.")
            break  # Stop flattening beyond the specified depth
        current_field = fields_to_process.pop(0)
        nested_df, additional_field = process_field(nested_df, current_field, current_depth, max_depth)

        if additional_field:
            fields_to_process.append(additional_field)

        complex_fields = get_complex_fields(nested_df)
        fields_to_process = update_fields_to_process(complex_fields, columns_to_explode)

        # Increment depth after processing all fields at the current level
        if not fields_to_process:
            current_depth += 1
            # Re-filter fields to process for the next depth level
            fields_to_process = filter_fields_to_process(complex_fields, columns_to_explode)
            logger.info(f"Incrementing depth to {current_depth}")

    logger.info("Flattening process completed.")
    return nested_df

# ===========================
# Example Usage with Complex DataFrames
# ===========================

def main():
    # Initialize SparkSession
    spark = SparkSession.builder \
        .appName("NestedDataFrameFlattening") \
        .getOrCreate()

    # Enable logging
    logger = logging.getLogger("FlattenNestedDataFrame")
    logger.setLevel(logging.INFO)

    # ===========================
    # Example 1: Deeply Nested Structs and Arrays
    # ===========================

    # Define schema for Example 1
    schema_example1 = StructType([
        StructField("customer_id", IntegerType(), True),
        StructField("customer_address", StructType([
            StructField("street", StringType(), True),
            StructField("city", StringType(), True),
            StructField("state", StringType(), True),
            StructField("zip_code", StringType(), True),
            StructField("geo", StructType([
                StructField("latitude", DoubleType(), True),
                StructField("longitude", DoubleType(), True)
            ]), True)
        ]), True),
        StructField("purchase_history", ArrayType(StructType([
            StructField("purchase_id", IntegerType(), True),
            StructField("product", StringType(), True),
            StructField("amount", DoubleType(), True),
            StructField("purchase_details", StructType([
                StructField("date", DateType(), True),
                StructField("location", StringType(), True),
                StructField("vendor", StructType([
                    StructField("name", StringType(), True),
                    StructField("rating", DoubleType(), True)
                ]), True)
            ]), True)
        ])), True),
        StructField("preferences", MapType(StringType(), StringType()), True)
    ])

    # Create sample data for Example 1
    data_example1 = [
        (
            1,
            {
                "street": "123 Elm St",
                "city": "Springfield",
                "state": "IL",
                "zip_code": "62704",
                "geo": {
                    "latitude": 39.7817,
                    "longitude": -89.6501
                }
            },
            [
                {
                    "purchase_id": 101,
                    "product": "Laptop",
                    "amount": 1200.50,
                    "purchase_details": {
                        "date": "2023-01-15",
                        "location": "Store A",
                        "vendor": {
                            "name": "TechCorp",
                            "rating": 4.5
                        }
                    }
                },
                {
                    "purchase_id": 102,
                    "product": "Mouse",
                    "amount": 25.75,
                    "purchase_details": {
                        "date": "2023-02-20",
                        "location": "Store B",
                        "vendor": {
                            "name": "GadgetWorld",
                            "rating": 4.2
                        }
                    }
                }
            ],
            {
                "newsletter": "subscribed",
                "sms_notifications": "enabled"
            }
        ),
        (
            2,
            {
                "street": "456 Oak St",
                "city": "Metropolis",
                "state": "NY",
                "zip_code": "10001",
                "geo": {
                    "latitude": 40.7128,
                    "longitude": -74.0060
                }
            },
            [
                {
                    "purchase_id": 103,
                    "product": "Keyboard",
                    "amount": 45.00,
                    "purchase_details": {
                        "date": "2023-03-10",
                        "location": "Store C",
                        "vendor": {
                            "name": "KeyMasters",
                            "rating": 4.7
                        }
                    }
                }
            ],
            {
                "newsletter": "unsubscribed",
                "sms_notifications": "disabled"
            }
        )
    ]

    # Create DataFrame for Example 1
    nested_df_example1 = spark.createDataFrame(data_example1, schema_example1)

    # Display Original DataFrame
    logger.info("Original DataFrame Example 1:")
    nested_df_example1.show(truncate=False)
    nested_df_example1.printSchema()

    # Flatten all nested structures
    flattened_df_all_example1 = flatten_nested_dataframe(nested_df_example1)
    logger.info("Flattened DataFrame Example 1 (All Levels):")
    flattened_df_all_example1.show(truncate=False)
    flattened_df_all_example1.printSchema()

    # Flatten specific columns up to depth 2
    flattened_df_specific_example1 = flatten_nested_dataframe(
        nested_df_example1,
        columns_to_explode=['purchase_history', 'customer_address'],
        max_depth=2
    )
    logger.info("Flattened DataFrame Example 1 (Specific Columns, Depth=2):")
    flattened_df_specific_example1.show(truncate=False)
    flattened_df_specific_example1.printSchema()

    # ===========================
    # Example 2: Nested Maps and Arrays
    # ===========================

    # Define schema for Example 2
    schema_example2 = StructType([
        StructField("order_id", IntegerType(), True),
        StructField("customer_info", StructType([
            StructField("customer_id", IntegerType(), True),
            StructField("name", StructType([
                StructField("first_name", StringType(), True),
                StructField("last_name", StringType(), True)
            ]), True),
            StructField("contact", MapType(StringType(), StringType()), True)
        ]), True),
        StructField("items", ArrayType(StructType([
            StructField("item_id", IntegerType(), True),
            StructField("description", StringType(), True),
            StructField("quantity", IntegerType(), True),
            StructField("price", DoubleType(), True),
            StructField("specs", MapType(StringType(), StringType()), True)
        ])), True),
        StructField("shipping_details", StructType([
            StructField("address", StructType([
                StructField("street", StringType(), True),
                StructField("city", StringType(), True),
                StructField("country", StringType(), True)
            ]), True),
            StructField("carrier", StringType(), True),
            StructField("tracking_number", StringType(), True)
        ]), True),
        StructField("order_date", DateType(), True)
    ])

    # Create sample data for Example 2
    data_example2 = [
        (
            501,
            {
                "customer_id": 1,
                "name": {
                    "first_name": "John",
                    "last_name": "Doe"
                },
                "contact": {
                    "email": "john.doe@example.com",
                    "phone": "555-1234"
                }
            },
            [
                {
                    "item_id": 1001,
                    "description": "Smartphone",
                    "quantity": 2,
                    "price": 599.99,
                    "specs": {
                        "color": "Black",
                        "memory": "128GB"
                    }
                },
                {
                    "item_id": 1002,
                    "description": "Headphones",
                    "quantity": 1,
                    "price": 199.99,
                    "specs": {
                        "color": "White",
                        "wireless": "Yes"
                    }
                }
            ],
            {
                "address": {
                    "street": "789 Maple Ave",
                    "city": "Star City",
                    "country": "USA"
                },
                "carrier": "FastShip",
                "tracking_number": "FS123456789US"
            },
            "2023-04-25"
        ),
        (
            502,
            {
                "customer_id": 2,
                "name": {
                    "first_name": "Jane",
                    "last_name": "Smith"
                },
                "contact": {
                    "email": "jane.smith@example.com",
                    "phone": "555-5678"
                }
            },
            [
                {
                    "item_id": 1003,
                    "description": "Laptop",
                    "quantity": 1,
                    "price": 1299.99,
                    "specs": {
                        "processor": "Intel i7",
                        "ram": "16GB"
                    }
                }
            ],
            {
                "address": {
                    "street": "456 Pine St",
                    "city": "Central City",
                    "country": "USA"
                },
                "carrier": "SpeedyExpress",
                "tracking_number": "SE987654321US"
            },
            "2023-05-10"
        )
    ]

    # Create DataFrame for Example 2
    nested_df_example2 = spark.createDataFrame(data_example2, schema_example2)

    # Display Original DataFrame
    logger.info("Original DataFrame Example 2:")
    nested_df_example2.show(truncate=False)
    nested_df_example2.printSchema()

    # Flatten all nested structures
    flattened_df_all_example2 = flatten_nested_dataframe(nested_df_example2)
    logger.info("Flattened DataFrame Example 2 (All Levels):")
    flattened_df_all_example2.show(truncate=False)
    flattened_df_all_example2.printSchema()

    # Flatten specific columns up to depth 1
    flattened_df_specific_example2 = flatten_nested_dataframe(
        nested_df_example2,
        columns_to_explode=['customer_info', 'items'],
        max_depth=1
    )
    logger.info("Flattened DataFrame Example 2 (Specific Columns, Depth=1):")
    flattened_df_specific_example2.show(truncate=False)
    flattened_df_specific_example2.printSchema()

    # Stop SparkSession
    spark.stop()

if __name__ == "__main__":
    main()
