from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, ArrayType,
    MapType, DoubleType, DateType
)
from pyspark.sql.functions import explode_outer, col, map_keys, map_values
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
# Example Usage with Six Nested DataFrames
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
    # Example 1: Employee Data
    # ===========================

    # Define schema for Example 1
    schema_example1 = StructType([
        StructField("employee_id", IntegerType(), True),
        StructField("personal_info", StructType([
            StructField("name", StructType([
                StructField("first_name", StringType(), True),
                StructField("last_name", StringType(), True)
            ]), True),
            StructField("contact", MapType(StringType(), StringType()), True)
        ]), True),
        StructField("job_info", StructType([
            StructField("title", StringType(), True),
            StructField("department", StringType(), True),
            StructField("manager", StructType([
                StructField("manager_id", IntegerType(), True),
                StructField("name", StringType(), True)
            ]), True)
        ]), True),
        StructField("projects", ArrayType(StructType([
            StructField("project_id", IntegerType(), True),
            StructField("project_name", StringType(), True)
        ])), True),
        StructField("preferences", MapType(StringType(), StringType()), True)
    ])

    # Create sample data for Example 1
    data_example1 = [
        (
            101,
            {
                "name": {
                    "first_name": "Alice",
                    "last_name": "Johnson"
                },
                "contact": {
                    "email": "alice.johnson@example.com",
                    "phone": "555-0101"
                }
            },
            {
                "title": "Software Engineer",
                "department": "Engineering",
                "manager": {
                    "manager_id": 201,
                    "name": "Bob Smith"
                }
            },
            [
                {
                    "project_id": 301,
                    "project_name": "Project Alpha"
                },
                {
                    "project_id": 302,
                    "project_name": "Project Beta"
                }
            ],
            {
                "remote_work": "enabled",
                "meal_allowance": "standard"
            }
        ),
        (
            102,
            {
                "name": {
                    "first_name": "Charlie",
                    "last_name": "Brown"
                },
                "contact": {
                    "email": "charlie.brown@example.com",
                    "phone": "555-0202"
                }
            },
            {
                "title": "Data Analyst",
                "department": "Data Science",
                "manager": {
                    "manager_id": 202,
                    "name": "Diana Prince"
                }
            },
            [
                {
                    "project_id": 303,
                    "project_name": "Project Gamma"
                }
            ],
            {
                "remote_work": "disabled",
                "meal_allowance": "premium"
            }
        )
    ]

    # Create DataFrame for Example 1
    nested_df_example1 = spark.createDataFrame(data_example1, schema_example1)

    # Display Original DataFrame
    logger.info("Original DataFrame Example 1: Employee Data")
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
        columns_to_explode=['personal_info', 'job_info', 'projects'],
        max_depth=2
    )
    logger.info("Flattened DataFrame Example 1 (Specific Columns, Depth=2):")
    flattened_df_specific_example1.show(truncate=False)
    flattened_df_specific_example1.printSchema()

    # ===========================
    # Example 2: Product Catalog
    # ===========================

    # Define schema for Example 2
    schema_example2 = StructType([
        StructField("product_id", IntegerType(), True),
        StructField("product_info", StructType([
            StructField("name", StringType(), True),
            StructField("specifications", MapType(StringType(), StringType()), True)
        ]), True),
        StructField("price", DoubleType(), True),
        StructField("categories", ArrayType(StringType()), True),
        StructField("supplier", StructType([
            StructField("supplier_id", IntegerType(), True),
            StructField("contact_info", StructType([
                StructField("email", StringType(), True),
                StructField("phone", StringType(), True)
            ]), True)
        ]), True)
    ])

    # Create sample data for Example 2
    data_example2 = [
        (
            501,
            {
                "name": "UltraWidget",
                "specifications": {
                    "weight": "2kg",
                    "dimensions": "30x20x10cm"
                }
            },
            99.99,
            ["Gadgets", "Widgets"],
            {
                "supplier_id": 601,
                "contact_info": {
                    "email": "supplier1@widgets.com",
                    "phone": "555-0303"
                }
            }
        ),
        (
            502,
            {
                "name": "MegaGadget",
                "specifications": {
                    "weight": "1.5kg",
                    "dimensions": "25x15x8cm"
                }
            },
            149.99,
            ["Gadgets", "Electronics"],
            {
                "supplier_id": 602,
                "contact_info": {
                    "email": "supplier2@gadgets.com",
                    "phone": "555-0404"
                }
            }
        )
    ]

    # Create DataFrame for Example 2
    nested_df_example2 = spark.createDataFrame(data_example2, schema_example2)

    # Display Original DataFrame
    logger.info("Original DataFrame Example 2: Product Catalog")
    nested_df_example2.show(truncate=False)
    nested_df_example2.printSchema()

    # Flatten all nested structures
    flattened_df_all_example2 = flatten_nested_dataframe(nested_df_example2)
    logger.info("Flattened DataFrame Example 2 (All Levels):")
    flattened_df_all_example2.show(truncate=False)
    flattened_df_all_example2.printSchema()

    # Flatten specific columns up to depth 2
    flattened_df_specific_example2 = flatten_nested_dataframe(
        nested_df_example2,
        columns_to_explode=['product_info', 'supplier'],
        max_depth=2
    )
    logger.info("Flattened DataFrame Example 2 (Specific Columns, Depth=2):")
    flattened_df_specific_example2.show(truncate=False)
    flattened_df_specific_example2.printSchema()

    # ===========================
    # Example 3: Student Records
    # ===========================

    # Define schema for Example 3
    schema_example3 = StructType([
        StructField("student_id", IntegerType(), True),
        StructField("personal_details", StructType([
            StructField("name", StructType([
                StructField("first_name", StringType(), True),
                StructField("last_name", StringType(), True)
            ]), True),
            StructField("address", StructType([
                StructField("street", StringType(), True),
                StructField("city", StringType(), True),
                StructField("country", StringType(), True)
            ]), True)
        ]), True),
        StructField("courses", ArrayType(StructType([
            StructField("course_id", IntegerType(), True),
            StructField("course_name", StringType(), True),
            StructField("grades", MapType(StringType(), StringType()), True)
        ])), True),
        StructField("enrollment_info", StructType([
            StructField("enrollment_date", DateType(), True),
            StructField("status", StringType(), True)
        ]), True),
        StructField("extracurricular", MapType(StringType(), StringType()), True)
    ])

    # Create sample data for Example 3
    data_example3 = [
        (
            1001,
            {
                "name": {
                    "first_name": "Emily",
                    "last_name": "Clark"
                },
                "address": {
                    "street": "12 College Ave",
                    "city": "University Town",
                    "country": "USA"
                }
            },
            [
                {
                    "course_id": 201,
                    "course_name": "Calculus",
                    "grades": {
                        "midterm": "A",
                        "final": "A-"
                    }
                },
                {
                    "course_id": 202,
                    "course_name": "Physics",
                    "grades": {
                        "midterm": "B+",
                        "final": "A"
                    }
                }
            ],
            {
                "enrollment_date": "2022-09-01",
                "status": "active"
            },
            {
                "chess_club": "member",
                "volunteer": "assistant"
            }
        ),
        (
            1002,
            {
                "name": {
                    "first_name": "Michael",
                    "last_name": "Lee"
                },
                "address": {
                    "street": "34 Campus Rd",
                    "city": "University Town",
                    "country": "USA"
                }
            },
            [
                {
                    "course_id": 203,
                    "course_name": "Chemistry",
                    "grades": {
                        "midterm": "A-",
                        "final": "B+"
                    }
                }
            ],
            {
                "enrollment_date": "2021-09-01",
                "status": "graduated"
            },
            {
                "basketball_team": "captain",
                "debate_club": "member"
            }
        )
    ]

    # Create DataFrame for Example 3
    nested_df_example3 = spark.createDataFrame(data_example3, schema_example3)

    # Display Original DataFrame
    logger.info("Original DataFrame Example 3: Student Records")
    nested_df_example3.show(truncate=False)
    nested_df_example3.printSchema()

    # Flatten all nested structures
    flattened_df_all_example3 = flatten_nested_dataframe(nested_df_example3)
    logger.info("Flattened DataFrame Example 3 (All Levels):")
    flattened_df_all_example3.show(truncate=False)
    flattened_df_all_example3.printSchema()

    # Flatten specific columns up to depth 2
    flattened_df_specific_example3 = flatten_nested_dataframe(
        nested_df_example3,
        columns_to_explode=['personal_details', 'courses'],
        max_depth=2
    )
    logger.info("Flattened DataFrame Example 3 (Specific Columns, Depth=2):")
    flattened_df_specific_example3.show(truncate=False)
    flattened_df_specific_example3.printSchema()

    # ===========================
    # Example 4: Sales Data
    # ===========================

    # Define schema for Example 4
    schema_example4 = StructType([
        StructField("sale_id", IntegerType(), True),
        StructField("customer", StructType([
            StructField("customer_id", IntegerType(), True),
            StructField("name", StringType(), True),
            StructField("contact", MapType(StringType(), StringType()), True)
        ]), True),
        StructField("items", ArrayType(StructType([
            StructField("item_id", IntegerType(), True),
            StructField("description", StringType(), True),
            StructField("quantity", IntegerType(), True),
            StructField("price", DoubleType(), True)
        ])), True),
        StructField("payment", StructType([
            StructField("method", StringType(), True),
            StructField("transaction_id", StringType(), True)
        ]), True),
        StructField("shipment", StructType([
            StructField("address", StructType([
                StructField("street", StringType(), True),
                StructField("city", StringType(), True),
                StructField("zip_code", StringType(), True)
            ]), True),
            StructField("carrier", StringType(), True),
            StructField("tracking_number", StringType(), True)
        ]), True)
    ])

    # Create sample data for Example 4
    data_example4 = [
        (
            2001,
            {
                "customer_id": 301,
                "name": "Sophia Martinez",
                "contact": {
                    "email": "sophia.martinez@example.com",
                    "phone": "555-0505"
                }
            },
            [
                {
                    "item_id": 401,
                    "description": "Wireless Mouse",
                    "quantity": 2,
                    "price": 25.99
                },
                {
                    "item_id": 402,
                    "description": "USB-C Adapter",
                    "quantity": 1,
                    "price": 15.49
                }
            ],
            {
                "method": "Credit Card",
                "transaction_id": "CC123456789"
            },
            {
                "address": {
                    "street": "56 Commerce St",
                    "city": "Business City",
                    "zip_code": "54321"
                },
                "carrier": "FastShip",
                "tracking_number": "FS987654321US"
            }
        ),
        (
            2002,
            {
                "customer_id": 302,
                "name": "Liam Nguyen",
                "contact": {
                    "email": "liam.nguyen@example.com",
                    "phone": "555-0606"
                }
            },
            [
                {
                    "item_id": 403,
                    "description": "Bluetooth Keyboard",
                    "quantity": 1,
                    "price": 45.00
                }
            ],
            {
                "method": "PayPal",
                "transaction_id": "PP987654321"
            },
            {
                "address": {
                    "street": "78 Market Ave",
                    "city": "Retail Town",
                    "zip_code": "67890"
                },
                "carrier": "SpeedyExpress",
                "tracking_number": "SE123456789US"
            }
        )
    ]

    # Create DataFrame for Example 4
    nested_df_example4 = spark.createDataFrame(data_example4, schema_example4)

    # Display Original DataFrame
    logger.info("Original DataFrame Example 4: Sales Data")
    nested_df_example4.show(truncate=False)
    nested_df_example4.printSchema()

    # Flatten all nested structures
    flattened_df_all_example4 = flatten_nested_dataframe(nested_df_example4)
    logger.info("Flattened DataFrame Example 4 (All Levels):")
    flattened_df_all_example4.show(truncate=False)
    flattened_df_all_example4.printSchema()

    # Flatten specific columns up to depth 2
    flattened_df_specific_example4 = flatten_nested_dataframe(
        nested_df_example4,
        columns_to_explode=['customer', 'items'],
        max_depth=2
    )
    logger.info("Flattened DataFrame Example 4 (Specific Columns, Depth=2):")
    flattened_df_specific_example4.show(truncate=False)
    flattened_df_specific_example4.printSchema()

    # ===========================
    # Example 5: Library System
    # ===========================

    # Define schema for Example 5
    schema_example5 = StructType([
        StructField("book_id", IntegerType(), True),
        StructField("title", StringType(), True),
        StructField("authors", ArrayType(StructType([
            StructField("author_id", IntegerType(), True),
            StructField("name", StringType(), True)
        ])), True),
        StructField("publication_info", StructType([
            StructField("publisher", StringType(), True),
            StructField("year", IntegerType(), True),
            StructField("location", StructType([
                StructField("city", StringType(), True),
                StructField("country", StringType(), True)
            ]), True)
        ]), True),
        StructField("availability", MapType(StringType(), StringType()), True)
    ])

    # Create sample data for Example 5
    data_example5 = [
        (
            3001,
            "Deep Learning Fundamentals",
            [
                {
                    "author_id": 501,
                    "name": "Dr. Alan Turing"
                },
                {
                    "author_id": 502,
                    "name": "Prof. Ada Lovelace"
                }
            ],
            {
                "publisher": "TechBooks Publishing",
                "year": 2020,
                "location": {
                    "city": "San Francisco",
                    "country": "USA"
                }
            },
            {
                "status": "available",
                "location": "Shelf A3"
            }
        ),
        (
            3002,
            "Introduction to Quantum Computing",
            [
                {
                    "author_id": 503,
                    "name": "Dr. Marie Curie"
                }
            ],
            {
                "publisher": "Science Press",
                "year": 2021,
                "location": {
                    "city": "Boston",
                    "country": "USA"
                }
            },
            {
                "status": "checked_out",
                "borrower": "student_12345"
            }
        )
    ]

    # Create DataFrame for Example 5
    nested_df_example5 = spark.createDataFrame(data_example5, schema_example5)

    # Display Original DataFrame
    logger.info("Original DataFrame Example 5: Library System")
    nested_df_example5.show(truncate=False)
    nested_df_example5.printSchema()

    # Flatten all nested structures
    flattened_df_all_example5 = flatten_nested_dataframe(nested_df_example5)
    logger.info("Flattened DataFrame Example 5 (All Levels):")
    flattened_df_all_example5.show(truncate=False)
    flattened_df_all_example5.printSchema()

    # Flatten specific columns up to depth 2
    flattened_df_specific_example5 = flatten_nested_dataframe(
        nested_df_example5,
        columns_to_explode=['authors', 'publication_info'],
        max_depth=2
    )
    logger.info("Flattened DataFrame Example 5 (Specific Columns, Depth=2):")
    flattened_df_specific_example5.show(truncate=False)
    flattened_df_specific_example5.printSchema()

    # ===========================
    # Example 6: IoT Device Data
    # ===========================

    # Define schema for Example 6
    schema_example6 = StructType([
        StructField("device_id", IntegerType(), True),
        StructField("device_info", StructType([
            StructField("manufacturer", StructType([
                StructField("name", StringType(), True),
                StructField("country", StringType(), True)
            ]), True),
            StructField("specifications", StructType([
                StructField("model", StringType(), True),
                StructField("features", MapType(StringType(), StringType()), True)
            ]), True)
        ]), True),
        StructField("usage_data", ArrayType(StructType([
            StructField("session_id", IntegerType(), True),
            StructField("metrics", StructType([
                StructField("temperature", DoubleType(), True),
                StructField("humidity", DoubleType(), True),
                StructField("location", StructType([
                    StructField("latitude", DoubleType(), True),
                    StructField("longitude", DoubleType(), True)
                ]), True)
            ]), True)
        ])), True)
    ])

    # Create sample data for Example 6
    data_example6 = [
        (
            4001,
            {
                "manufacturer": {
                    "name": "SmartTech",
                    "country": "USA"
                },
                "specifications": {
                    "model": "ST-X100",
                    "features": {
                        "battery_life": "24h",
                        "water_resistant": "Yes"
                    }
                }
            },
            [
                {
                    "session_id": 501,
                    "metrics": {
                        "temperature": 22.5,
                        "humidity": 45.0,
                        "location": {
                            "latitude": 37.7749,
                            "longitude": -122.4194
                        }
                    }
                },
                {
                    "session_id": 502,
                    "metrics": {
                        "temperature": 23.0,
                        "humidity": 50.0,
                        "location": {
                            "latitude": 34.0522,
                            "longitude": -118.2437
                        }
                    }
                }
            ]
        ),
        (
            4002,
            {
                "manufacturer": {
                    "name": "EcoDevices",
                    "country": "Germany"
                },
                "specifications": {
                    "model": "ED-Z200",
                    "features": {
                        "battery_life": "30h",
                        "water_resistant": "No"
                    }
                }
            },
            [
                {
                    "session_id": 503,
                    "metrics": {
                        "temperature": 21.0,
                        "humidity": 40.0,
                        "location": {
                            "latitude": 48.1351,
                            "longitude": 11.5820
                        }
                    }
                }
            ]
        )
    ]

    # Create DataFrame for Example 6
    nested_df_example6 = spark.createDataFrame(data_example6, schema_example6)

    # Display Original DataFrame
    logger.info("Original DataFrame Example 6: IoT Device Data")
    nested_df_example6.show(truncate=False)
    nested_df_example6.printSchema()

    # Flatten all nested structures
    flattened_df_all_example6 = flatten_nested_dataframe(nested_df_example6)
    logger.info("Flattened DataFrame Example 6 (All Levels):")
    flattened_df_all_example6.show(truncate=False)
    flattened_df_all_example6.printSchema()

    # Flatten specific columns up to depth 3
    flattened_df_specific_example6 = flatten_nested_dataframe(
        nested_df_example6,
        columns_to_explode=['device_info', 'usage_data'],
        max_depth=3
    )
    logger.info("Flattened DataFrame Example 6 (Specific Columns, Depth=3):")
    flattened_df_specific_example6.show(truncate=False)
    flattened_df_specific_example6.printSchema()

    # Stop SparkSession
    spark.stop()

if __name__ == "__main__":
    main()
