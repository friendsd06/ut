from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, ArrayType, DoubleType

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

    def process_field(parent, field, field_type):
        nonlocal alias_counter
        if isinstance(field_type, StructType):
            for subfield in field_type.fields:
                full_field = f"{parent}.{field}.{subfield.name}" if parent else f"{field}.{subfield.name}"
                alias_name = full_field.replace('.', '_')
                process_field(parent=f"{parent}.{field}" if parent else field, field=subfield.name, field_type=subfield.dataType)
        elif isinstance(field_type, ArrayType):
            full_field = f"{parent}.{field}" if parent else field
            should_explode = full_field in explode_columns or not explode_columns
            if should_explode:
                alias_counter += 1
                exploded_alias = f"{field}_exploded_{alias_counter}"
                lateral_view_clauses.append(f"LATERAL VIEW OUTER EXPLODE(COALESCE(`{full_field}`, ARRAY(NULL))) {exploded_alias} AS `{exploded_alias}`")

                if isinstance(field_type.elementType, StructType):
                    for subfield in field_type.elementType.fields:
                        sub_full_field = f"{exploded_alias}.{subfield.name}"
                        alias_name = f"{full_field}_{subfield.name}"
                        select_expressions.append(f"`{sub_full_field}` AS `{alias_name}`")
                        process_field(parent=exploded_alias, field=subfield.name, field_type=subfield.dataType)
                else:
                    select_expressions.append(f"`{exploded_alias}` AS `{full_field}`")
            else:
                alias_name = full_field.replace('.', '_')
                select_expressions.append(f"`{full_field}` AS `{alias_name}`")
        else:
            full_field = f"{parent}.{field}" if parent else field
            alias_name = full_field.replace('.', '_')
            select_expressions.append(f"`{full_field}` AS `{alias_name}`")

    for field in df.schema.fields:
        process_field(parent="", field=field.name, field_type=field.dataType)

    select_clause = ", ".join(select_expressions)
    lateral_view_clause = " ".join(lateral_view_clauses)
    sql_query = f"SELECT {select_clause} FROM `{table_name}` {lateral_view_clause}"

    return sql_query

# Sample complex nested data
data = [
    (
        1,
        {"name": "John Doe", "age": 30, "contact": {"email": "john@example.com", "phone": "123-456-7890"}},
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
                    "billing_address": {"street": "123 Main St", "city": "New York", "zip": "10001"}
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
        ]
    ),
    (
        2,
        {"name": "Alice Smith", "age": 28, "contact": {"email": "alice@example.com", "phone": "987-654-3210"}},
        [
            {
                "order_id": 103,
                "order_items": [
                    {"product": "Desk", "quantity": 1}
                ],
                "payment_details": {
                    "method": "Credit Card",
                    "card": {"type": "MasterCard", "number": "****-5678"},
                    "billing_address": {"street": "456 Elm St", "city": "Los Angeles", "zip": "90001"}
                }
            }
        ],
        [
            {"category": "office", "tags": ["work", "productivity"]}
        ]
    )
]

# Define schema for the complex nested structure
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
    ])), True)
])

# Create DataFrame from sample data
df = spark.createDataFrame(data, schema)

# Register DataFrame as a temporary SQL table
df.createOrReplaceTempView("complex_nested_table")

# Specify columns to explode
columns_to_explode = ["orders", "orders.order_items", "preferences"]

# Generate the flatten SQL query
sql_query = generate_flatten_sql(df, "complex_nested_table", columns_to_explode)
print("Generated SQL Query:\n", sql_query)

# Execute the generated SQL query
flattened_df = spark.sql(sql_query)
flattened_df.show(truncate=False)
flattened_df.printSchema()

# Stop Spark session
spark.stop()