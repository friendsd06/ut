from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    ArrayType,
    DoubleType,
    BooleanType,
)

# Initialize SparkSession
spark = SparkSession.builder \
    .appName("ComplexNestedFlattening") \
    .master("local[*]") \
    .getOrCreate()

def generate_flatten_sql(df, table_name, columns_to_explode=None):
    """
    Generates a SQL query to flatten a nested DataFrame schema, with selective column explosion.

    :param df: Input DataFrame with nested schema
    :param table_name: Name of the table to generate SQL for
    :param columns_to_explode: List of column names to explode (dot notation for nested fields)
    :return: Flattened SQL query as a string
    """
    select_expressions = []
    lateral_view_clauses = []
    alias_counter = 0
    explode_columns = set(columns_to_explode or [])

    def process_field(original_path, current_parent, field, field_type):
        nonlocal alias_counter
        original_full_field = f"{original_path}.{field}" if original_path else field
        current_full_field = f"{current_parent}.{field}" if current_parent else field

        if isinstance(field_type, StructType):
            for subfield in field_type.fields:
                process_field(original_full_field, current_full_field, subfield.name, subfield.dataType)
        elif isinstance(field_type, ArrayType):
            should_explode = original_full_field in explode_columns or not explode_columns

            if should_explode:
                alias_counter += 1
                exploded_alias = f"{field}_exploded_{alias_counter}"
                if isinstance(field_type.elementType, StructType):
                    # Explode the array of structs
                    explode_clause = (
                        f"LATERAL VIEW OUTER EXPLODE({current_full_field}) {exploded_alias} AS `{exploded_alias}`"
                    )
                    lateral_view_clauses.append(explode_clause)
                    for subfield in field_type.elementType.fields:
                        # Build the original path for the subfield
                        sub_original_path = original_full_field
                        # The current parent is the exploded alias
                        sub_current_parent = exploded_alias
                        # The field is subfield.name
                        sub_field = subfield.name
                        # Add to select_expressions with proper aliasing
                        alias_name = f"{field}_{sub_field}_{alias_counter}"
                        select_expressions.append(
                            f"{exploded_alias}.`{sub_field}` AS `{alias_name}`"
                        )
                        # Recursively process subfields
                        process_field(
                            sub_original_path, sub_current_parent, sub_field, subfield.dataType
                        )
                else:
                    # Explode the array of primitive types
                    explode_clause = (
                        f"LATERAL VIEW OUTER EXPLODE({current_full_field}) {exploded_alias} AS `{exploded_alias}`"
                    )
                    lateral_view_clauses.append(explode_clause)
                    alias_name = f"{field}_exploded_{alias_counter}"
                    select_expressions.append(f"`{exploded_alias}` AS `{alias_name}`")
            else:
                # If not exploded, keep the array as is (optionally sorted)
                alias_name = f"{current_full_field.replace('.', '_')}"
                select_expressions.append(
                    f"SORT_ARRAY({current_full_field}) AS `{alias_name}`"
                )
        else:
            # Primitive field, select with alias
            if current_parent:
                alias_name = f"{current_full_field.replace('.', '_')}"
            else:
                alias_name = field
            select_expressions.append(f"{current_full_field} AS `{alias_name}`")

    for field in df.schema.fields:
        process_field("", "", field.name, field.dataType)

    select_clause = ", ".join(select_expressions)
    lateral_view_clause = " ".join(lateral_view_clauses)
    sql_query = f"SELECT {select_clause} FROM `{table_name}` {lateral_view_clause}"

    return sql_query

# Sample complex nested data with more nested structures
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
    # Add more test data here if needed
]

# Define schema for the complex nested structure
schema = StructType([
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
                StructField("zip", StringType()), True)
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

# Create DataFrame from sample data
df = spark.createDataFrame(data, schema)

# Register DataFrame as a temporary SQL table
df.createOrReplaceTempView("complex_nested_table")

# Specify columns to explode
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

# Generate the flatten SQL query
sql_query = generate_flatten_sql(df, "complex_nested_table", columns_to_explode)
print("Generated SQL Query:\n", sql_query)

# Execute the generated SQL query
flattened_df = spark.sql(sql_query)
flattened_df.show(truncate=False)
flattened_df.printSchema()

# Stop Spark session
spark.stop()
