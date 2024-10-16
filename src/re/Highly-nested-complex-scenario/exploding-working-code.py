# Import necessary libraries
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    ArrayType,
    DoubleType,
)
from pyspark.sql.functions import explode_outer, sort_array, col

def initialize_spark(app_name: str = "ComplexNestedFlattening") -> SparkSession:
    """
    Initializes and returns a SparkSession.

    :param app_name: Name of the Spark application.
    :return: SparkSession object.
    """
    return SparkSession.builder \
        .appName(app_name) \
        .master("local[*]") \
        .getOrCreate()

def define_schema() -> StructType:
    """
    Defines and returns the schema for the complex nested DataFrame.

    :return: StructType representing the schema.
    """
    return StructType([
        StructField("customer_id", IntegerType(), False),
        StructField("customer_info", StructType([
            StructField("name", StringType(), True),
            StructField("age", IntegerType(), True),
            StructField("contact", StructType([
                StructField("email", StringType(), True),
                StructField("phone", StringType(), True),
                StructField("address", StructType([
                    StructField("street", StringType(), True),
                    StructField("city", StringType(), True),
                    StructField("zip", StringType(), True)
                ]), True)
            ]), True),
            StructField("skills", ArrayType(StringType()), True)
        ]), True),
        StructField("orders", ArrayType(StructType([
            StructField("order_id", IntegerType(), True),
            StructField("order_date", StringType(), True),
            StructField("order_items", ArrayType(StructType([
                StructField("product", StringType(), True),
                StructField("quantity", IntegerType(), True),
                StructField("price", DoubleType(), True),
                StructField("specs", StructType([
                    StructField("brand", StringType(), True),
                    StructField("model", StringType(), True),
                    StructField("features", ArrayType(StringType()), True)
                ]), True)
            ])), True),
            StructField("payment_details", StructType([
                StructField("method", StringType(), True),
                StructField("card", StructType([
                    StructField("type", StringType(), True),
                    StructField("number", StringType(), True),
                    StructField("expiry", StringType(), True)
                ]), True),
                StructField("account", StringType(), True),
                StructField("billing_address", StructType([
                    StructField("street", StringType(), True),
                    StructField("city", StringType(), True),
                    StructField("zip", StringType(), True)
                ]), True)
            ]), True),
            StructField("shipping", StructType([
                StructField("method", StringType(), True),
                StructField("tracking", ArrayType(StringType()), True),
                StructField("address", StructType([
                    StructField("street", StringType(), True),
                    StructField("city", StringType(), True),
                    StructField("zip", StringType(), True)
                ]), True)
            ]), True)
        ])), True),
        StructField("preferences", ArrayType(StructType([
            StructField("category", StringType(), True),
            StructField("tags", ArrayType(StringType()), True),
            StructField("priority", IntegerType(), True)
        ])), True),
        StructField("lists", StructType([
            StructField("favorites", ArrayType(StringType()), True),
            StructField("wishlist", ArrayType(StringType()), True)
        ]), True)
    ])

def create_sample_data(schema: StructType):
    """
    Creates and returns a DataFrame with sample complex nested data.

    :param schema: StructType defining the schema of the DataFrame.
    :return: DataFrame with sample data.
    """
    data = [
        (
            1,
            {
                "name": "John Doe",
                "age": 30,
                "contact": {
                    "email": "john@example.com",
                    "phone": "123-456-7890",
                    "address": {
                        "street": "123 Main St",
                        "city": "New York",
                        "zip": "10001"
                    }
                },
                "skills": ["Python", "Spark", "SQL"]
            },
            [
                {
                    "order_id": 101,
                    "order_date": "2023-01-15",
                    "order_items": [
                        {
                            "product": "Laptop",
                            "quantity": 1,
                            "price": 1200.00,
                            "specs": {
                                "brand": "Dell",
                                "model": "XPS",
                                "features": ["16GB RAM", "512GB SSD"]
                            }
                        },
                        {
                            "product": "Mouse",
                            "quantity": 2,
                            "price": 25.50,
                            "specs": {
                                "brand": "Logitech",
                                "model": "MX",
                                "features": ["Wireless", "Ergonomic"]
                            }
                        }
                    ],
                    "payment_details": {
                        "method": "Credit Card",
                        "card": {
                            "type": "Visa",
                            "number": "****-1234",
                            "expiry": "12/25"
                        },
                        "billing_address": {
                            "street": "123 Main St",
                            "city": "New York",
                            "zip": "10001"
                        }
                    },
                    "shipping": {
                        "method": "Express",
                        "tracking": ["ABC123", "DEF456"],
                        "address": {
                            "street": "123 Main St",
                            "city": "New York",
                            "zip": "10001"
                        }
                    }
                },
                {
                    "order_id": 102,
                    "order_date": "2023-02-20",
                    "order_items": [
                        {
                            "product": "Keyboard",
                            "quantity": 1,
                            "price": 100.00,
                            "specs": {
                                "brand": "Corsair",
                                "model": "K95",
                                "features": ["Mechanical", "RGB"]
                            }
                        }
                    ],
                    "payment_details": {
                        "method": "PayPal",
                        "account": "john@paypal.com"
                    },
                    "shipping": {
                        "method": "Standard",
                        "tracking": ["GHI789"],
                        "address": {
                            "street": "456 Elm St",
                            "city": "Los Angeles",
                            "zip": "90001"
                        }
                    }
                }
            ],
            [
                {"category": "electronics", "tags": ["tech", "gadgets"], "priority": 1},
                {"category": "home office", "tags": ["work", "productivity"], "priority": 2}
            ],
            {"favorites": ["Laptop", "Keyboard"], "wishlist": ["Monitor", "Headphones"]}
        ),
        # You can add more test data here if needed
    ]

    return spark.createDataFrame(data, schema)

def flatten_dataframe(df, columns_to_explode):
    """
    Flattens a nested DataFrame by selectively exploding specified columns.

    :param df: Input DataFrame with nested schema.
    :param columns_to_explode: List of column names to explode (dot notation for nested fields).
    :return: Flattened DataFrame.
    """
    for column in columns_to_explode:
        # Split the column path to identify parent and field
        parts = column.split('.')
        parent = '.'.join(parts[:-1]) if len(parts) > 1 else parts[0]
        field = parts[-1]

        # Define the alias for the exploded column
        exploded_alias = f"{field}_exploded"
        print(f"Exploding column: '{column}' as '{exploded_alias}'")

        # Apply explode_outer to handle nulls and empty arrays
        df = df.withColumn(exploded_alias, explode_outer(col(column)))

        # Retrieve the data type of the exploded column
        try:
            # Traverse the schema to get the data type
            exploded_field = df.schema
            for part in parts:
                exploded_field = exploded_field[part].dataType
            if isinstance(exploded_field, ArrayType):
                exploded_field = exploded_field.elementType

            if isinstance(exploded_field, StructType):
                # If the exploded field is a StructType, expand its subfields
                for subfield in exploded_field.fields:
                    subfield_name = subfield.name
                    new_col_name = f"{field}_{subfield_name}_exploded"
                    df = df.withColumn(new_col_name, col(f"{exploded_alias}.{subfield_name}"))
                    print(f"Added column: '{new_col_name}' from '{exploded_alias}.{subfield_name}'")
                # Drop the exploded struct column after expanding
                df = df.drop(exploded_alias)
            else:
                # If the exploded field is a primitive type, rename the exploded column
                new_col_name = f"{field}_exploded"
                df = df.withColumnRenamed(exploded_alias, new_col_name)
                print(f"Renamed column: '{exploded_alias}' to '{new_col_name}'")
        except Exception as e:
            print(f"Error processing column '{column}': {e}")

    # Function to recursively collect and alias all columns
    def get_all_columns(schema, prefix=""):
        """
        Recursively collects all columns from the schema, replacing dots with underscores.

        :param schema: StructType schema of the DataFrame.
        :param prefix: Current prefix for nested fields.
        :return: List of column expressions.
        """
        fields = []
        for field in schema.fields:
            col_name = f"{prefix}.{field.name}" if prefix else field.name
            if isinstance(field.dataType, StructType):
                # Recursively process StructType fields
                fields += get_all_columns(field.dataType, prefix=col_name)
            elif isinstance(field.dataType, ArrayType) and not isinstance(field.dataType.elementType, StructType):
                # Sort arrays of primitive types and alias them
                fields.append(sort_array(col(col_name)).alias(col_name.replace('.', '_')))
            else:
                # Alias all other columns, replacing dots with underscores
                fields.append(col(col_name).alias(col_name.replace('.', '_')))
        return fields

    # Collect all flattened columns with proper aliasing
    flattened_columns = get_all_columns(df.schema)

    # Select all columns to create the flattened DataFrame
    return df.select(*flattened_columns)

def main():
    """
    Main function to execute the flattening process.
    """
    # Initialize SparkSession
    spark = initialize_spark()

    # Define the schema
    schema = define_schema()

    # Create DataFrame with sample data
    df = create_sample_data(schema)

    # (Optional) Register DataFrame as a temporary SQL view
    df.createOrReplaceTempView("complex_nested_table")

    # Define columns to explode using dot notation for nested fields
    columns_to_explode = [
        "orders",
        "orders.order_items",
        "orders.order_items.specs.features",
        "orders.shipping.tracking",
        "preferences",
        "preferences.tags",
        "customer_info.skills",
        "lists.favorites",
        "lists.wishlist"
    ]

    # Flatten the DataFrame
    flattened_df = flatten_dataframe(df, columns_to_explode)

    # Display the flattened DataFrame
    print("\nFlattened DataFrame:")
    flattened_df.show(truncate=False)

    # Display the schema of the flattened DataFrame
    print("\nSchema of Flattened DataFrame:")
    flattened_df.printSchema()

    # Stop SparkSession
    spark.stop()

# Execute the main function
main()
