from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, ArrayType, DoubleType

# Initialize SparkSession
spark = SparkSession.builder \
    .appName("ComplexNestedFlattening") \
    .master("local[*]") \
    .getOrCreate()

def generate_flatten_sql(df, table_name):
    """
    Generates a SQL query to flatten all nested columns (structs and arrays) in a DataFrame.

    Args:
        df (DataFrame): The input DataFrame with nested columns.
        table_name (str): The temporary table name for the DataFrame in SQL.

    Returns:
        str: A SQL query that flattens all nested columns.
    """
    select_expressions = []
    lateral_view_clauses = []
    alias_counter = 0  # To ensure unique aliases for exploded arrays

    def process_field(parent, field, field_type):
        """
        Recursively processes each field to generate SELECT expressions and LATERAL VIEW clauses.

        Args:
            parent (str): The parent field path.
            field (str): The current field name.
            field_type (DataType): The Spark DataType of the current field.
        """
        nonlocal alias_counter

        # Construct the full field path and alias prefix
        if parent:
            full_field = f"{parent}.`{field}`"
            alias_prefix = f"{parent}_{field}"
        else:
            full_field = f"`{field}`"
            alias_prefix = f"{field}"

        if isinstance(field_type, StructType):
            # Handle StructType by processing its subfields
            for subfield in field_type.fields:
                process_field(parent=alias_prefix, field=subfield.name, field_type=subfield.dataType)
        elif isinstance(field_type, ArrayType):
            element_type = field_type.elementType
            if isinstance(element_type, StructType):
                # Handle Array of Structs: Explode and process subfields
                alias_counter += 1
                exploded_alias = f"{alias_prefix}_exploded_{alias_counter}"

                if parent:
                    explode_clause = f"LATERAL VIEW OUTER EXPLODE({parent}.`{field}`) {exploded_alias} AS `{exploded_alias}`"
                else:
                    explode_clause = f"LATERAL VIEW OUTER EXPLODE(`{field}`) {exploded_alias} AS `{exploded_alias}`"
                lateral_view_clauses.append(explode_clause)

                # Process each subfield within the exploded struct
                for subfield in element_type.fields:
                    process_field(parent=exploded_alias, field=subfield.name, field_type=subfield.dataType)
            else:
                # Handle Array of Scalars: Sort and alias
                if parent:
                    alias_name = f"{parent}_{field}_sorted"
                else:
                    alias_name = f"{field}_sorted"
                # Sort the array to ensure order-independent comparison
                sort_expr = f"SORT_ARRAY({full_field}, TRUE) AS `{alias_name}`"
                select_expressions.append(sort_expr)
        else:
            # Handle Scalar Types: Alias the field
            if parent:
                alias_name = f"{parent}_{field}"
            else:
                alias_name = field
            select_expressions.append(f"{full_field} AS `{alias_name}`")

    # Start processing top-level fields
    for field in df.schema.fields:
        process_field(parent="", field=field.name, field_type=field.dataType)

    # Combine SELECT expressions
    select_clause = ",\n  ".join(select_expressions)

    # Combine LATERAL VIEW clauses
    lateral_view_clause = " ".join(lateral_view_clauses)

    # Construct the final SQL query
    if lateral_view_clause:
        sql_query = f"SELECT \n  {select_clause} \nFROM `{table_name}` {lateral_view_clause}"
    else:
        sql_query = f"SELECT \n  {select_clause} \nFROM `{table_name}`"

    return sql_query

# Sample complex nested data including 'grades' field
data = [
    (
        1,
        {
            "name": "John Doe",
            "age": 30,
            "contact": {
                "email": "john@example.com",
                "phone": "123-456-7890"
            }
        },
        [
            {
                "order_id": 101,
                "order_items": [
                    {"product": "Laptop", "quantity": 1},
                    {"product": "Mouse", "quantity": 2}
                ],
                "payment_details": {
                    "method": "Credit Card",
                    "card": {"type": "Visa", "number": "****-1234"},
                    "billing_address": {
                        "street": "123 Main St",
                        "city": "New York",
                        "zip": "10001"
                    }
                }
            },
            {
                "order_id": 102,
                "order_items": [
                    {"product": "Keyboard", "quantity": 1}
                ],
                "payment_details": {
                    "method": "PayPal",
                    "account": "john@paypal.com"
                }
            }
        ],
        [
            {"category": "electronics", "tags": ["tech", "gadgets"]},
            {"category": "home", "tags": ["decor", "furniture"]}
        ],
        {
            "scores": [85, 90, 78],
            "average": 84.3
        }
    ),
    (
        2,
        {
            "name": "Alice Smith",
            "age": 28,
            "contact": {
                "email": "alice@example.com",
                "phone": "987-654-3210"
            }
        },
        [
            {
                "order_id": 103,
                "order_items": [
                    {"product": "Desk", "quantity": 1}
                ],
                "payment_details": {
                    "method": "Credit Card",
                    "card": {"type": "MasterCard", "number": "****-5678"},
                    "billing_address": {
                        "street": "456 Elm St",
                        "city": "Los Angeles",
                        "zip": "90001"
                    }
                }
            }
        ],
        [
            {"category": "office", "tags": ["work", "productivity"]}
        ],
        {
            "scores": [92, 88, 95],
            "average": 91.7
        }
    )
]

# Define schema for the complex nested structure, including 'grades' field
schema = StructType([
    StructField("customer_id", IntegerType(), False),
    StructField("customer_info", StructType([
        StructField("name", StringType(), True),
        StructField("age", IntegerType(), True),
        StructField("contact", StructType([
            StructField("email", StringType(), True),
            StructField("phone", StringType(), True)
        ]), True)
    ]), True),
    StructField("orders", ArrayType(StructType([
        StructField("order_id", IntegerType(), True),
        StructField("order_items", ArrayType(StructType([
            StructField("product", StringType(), True),
            StructField("quantity", IntegerType(), True)
        ])), True),
        StructField("payment_details", StructType([
            StructField("method", StringType(), True),
            StructField("card", StructType([
                StructField("type", StringType(), True),
                StructField("number", StringType(), True)
            ]), True),
            StructField("account", StringType(), True),
            StructField("billing_address", StructType([
                StructField("street", StringType(), True),
                StructField("city", StringType(), True),
                StructField("zip", StringType(), True)
            ]), True)
        ]), True)
    ])), True),
    StructField("preferences", ArrayType(StructType([
        StructField("category", StringType(), True),
        StructField("tags", ArrayType(StringType()), True)
    ])), True),
    StructField("grades", StructType([
        StructField("scores", ArrayType(IntegerType()), True),
        StructField("average", DoubleType(), True)
    ]), True)
])

# Create DataFrame from sample data
df = spark.createDataFrame(data, schema)

# Register DataFrame as a temporary SQL table
df.createOrReplaceTempView("complex_nested_table")

# Generate the flatten SQL query
sql_query = generate_flatten_sql(df, "complex_nested_table")
print("Generated SQL Query:\n", sql_query)

# Execute the generated SQL query
flattened_df = spark.sql(sql_query)

# Display the flattened DataFrame
flattened_df.show(truncate=False)
flattened_df.printSchema()

# Stop Spark session
spark.stop()
