from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, ArrayType

# Initialize SparkSession
spark = SparkSession.builder \
    .appName("ComplexNestedFlattening") \
    .master("local[*]") \
    .getOrCreate()

def generate_flatten_sql(df, table_name, columns_to_explode=None):
    """
    Generates a SQL query to flatten a nested DataFrame schema, handling deep nesting.

    :param df: Input DataFrame with nested schema
    :param table_name: Name of the table to generate SQL for
    :param columns_to_explode: List of column names to explode in specified order
    :return: Flattened SQL query as a string
    """
    select_expressions = []
    lateral_view_clauses = []
    explode_columns = set(columns_to_explode or [])

    def process_field(field_path, data_type, current_depth=0, parent_exploded=False):
        nonlocal select_expressions, lateral_view_clauses

        if isinstance(data_type, StructType):
            for subfield in data_type.fields:
                sub_path = f"{field_path}.{subfield.name}" if field_path else subfield.name
                process_field(sub_path, subfield.dataType, current_depth, parent_exploded)
        elif isinstance(data_type, ArrayType):
            should_explode = field_path in explode_columns or not explode_columns
            if should_explode and not parent_exploded:
                alias = f"exploded_{field_path.replace('.', '_')}_{current_depth}"
                lateral_view_clauses.append(f"LATERAL VIEW EXPLODE(COALESCE(`{field_path}`, ARRAY(NULL))) {alias} AS `{alias}`")
                process_field(alias, data_type.elementType, current_depth + 1, True)
            else:
                alias = field_path.replace('.', '_')
                select_expressions.append(f"`{field_path}` AS `{alias}`")
                if isinstance(data_type.elementType, (StructType, ArrayType)):
                    process_field(field_path, data_type.elementType, current_depth, parent_exploded)
        else:
            alias = field_path.replace('.', '_')
            select_expressions.append(f"`{field_path}` AS `{alias}`")

    for field in df.schema.fields:
        process_field(field.name, field.dataType)

    select_clause = ",\n    ".join(select_expressions)
    lateral_view_clause = "\n    ".join(lateral_view_clauses)
    sql_query = f"SELECT DISTINCT\n    {select_clause}\nFROM `{table_name}`\n    {lateral_view_clause}"

    return sql_query

# Sample data structure with deep nesting
data = [
    (
        1,
        {"name": "John Doe", "age": 30, "contact": {"email": "john@example.com", "phone": "123-456-7890"}},
        [
            {
                "order_id": 101,
                "order_items": [
                    {"product": "Laptop", "quantity": 1, "details": {"brand": "Dell", "specs": ["i7", "16GB RAM"]}},
                    {"product": "Mouse", "quantity": 2, "details": {"brand": "Logitech", "specs": ["Wireless"]}}
                ],
                "payment_details": {
                    "method": "Credit Card",
                    "billing_address": {"city": "New York", "zip": "10001"}
                }
            },
            {
                "order_id": 102,
                "order_items": [
                    {"product": "Keyboard", "quantity": 1, "details": {"brand": "Corsair", "specs": ["Mechanical"]}}
                ],
                "payment_details": {
                    "method": "PayPal",
                    "billing_address": {"city": "Los Angeles", "zip": "90001"}
                }
            }
        ],
        [
            {"category": "electronics", "tags": ["tech", "gadgets"]},
            {"category": "accessories", "tags": ["computer"]}
        ]
    )
]

# Define schema with deep nesting
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
            StructField("quantity", IntegerType(), True),
            StructField("details", StructType([
                StructField("brand", StringType(), True),
                StructField("specs", ArrayType(StringType()), True)
            ]), True)
        ])), True),
        StructField("payment_details", StructType([
            StructField("method", StringType(), True),
            StructField("billing_address", StructType([
                StructField("city", StringType(), True),
                StructField("zip", StringType(), True)
            ]), True)
        ]), True)
    ])), True),
    StructField("preferences", ArrayType(StructType([
        StructField("category", StringType(), True),
        StructField("tags", ArrayType(StringType()), True)
    ])), True)
])

# Create DataFrame
df = spark.createDataFrame(data, schema)

# Register DataFrame as a temporary SQL table
df.createOrReplaceTempView("complex_nested_table")

# Specify columns to explode
columns_to_explode = ["orders", "orders.order_items", "orders.order_items.details.specs", "preferences", "preferences.tags"]

# Generate and print the flatten SQL query
sql_query = generate_flatten_sql(df, "complex_nested_table", columns_to_explode)
print("Generated SQL Query:\n")
print(sql_query)

# Execute the generated SQL query
flattened_df = spark.sql(sql_query)

# Show the flattened DataFrame
print("\nFlattened DataFrame:")
flattened_df.show(truncate=False)

# Print the schema of the flattened DataFrame
print("\nFlattened DataFrame Schema:")
flattened_df.printSchema()

# Stop Spark session
spark.stop()