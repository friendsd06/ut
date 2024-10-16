# Import necessary PySpark modules
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr, when, lit
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, ArrayType, DoubleType
)

# Initialize SparkSession
spark = SparkSession.builder \
    .appName("IterativeFlattening") \
    .master("local[*]") \
    .getOrCreate()

def generate_flatten_sql(df, table_name, columns_to_explode=None):
    """
    Generates a SQL query to flatten a nested DataFrame schema iteratively using a stack.

    :param df: Input DataFrame with nested schema
    :param table_name: Name of the table to generate SQL for
    :param columns_to_explode: List of column names to explode. If None, explode all array columns.
    :return: Flattened SQL query as a string
    """
    select_expressions = []
    lateral_view_clauses = []
    alias_counter = 0

    # Stack will hold tuples of (parent, field_name, field_type)
    stack = [(None, field.name, field.dataType) for field in df.schema.fields]

    # Set of columns to explode for quick lookup
    explode_columns = set(columns_to_explode) if columns_to_explode else set()

    while stack:
        parent, field, field_type = stack.pop()
        full_field = f"{parent}.{field}" if parent else field

        if isinstance(field_type, StructType):
            for subfield in field_type.fields:
                # Generate the full field path
                sub_full_field = f"{full_field}.{subfield.name}"
                # Generate alias name
                alias_name = f"{full_field.replace('.', '_')}_{subfield.name}"
                # Add to select expressions
                select_expressions.append(f"{sub_full_field} AS `{alias_name}`")
                # Push subfield to the stack for further processing
                stack.append((full_field, subfield.name, subfield.dataType))

        elif isinstance(field_type, ArrayType):
            # Determine if this column needs to be exploded
            should_explode = not columns_to_explode or field in explode_columns
            if should_explode:
                alias_counter += 1
                exploded_alias = f"{field}_exploded_{alias_counter}"

                # Add LATERAL VIEW OUTER EXPLODE clause
                explode_clause = f"LATERAL VIEW OUTER EXPLODE(`{full_field}`) {exploded_alias} AS `{exploded_alias}`"
                lateral_view_clauses.append(explode_clause)

                element_type = field_type.elementType
                if isinstance(element_type, StructType):
                    for subfield in element_type.fields:
                        # Generate the full field path from the exploded array
                        sub_full_field = f"{exploded_alias}.{subfield.name}"
                        # Generate alias name
                        alias_name = f"{exploded_alias}_{subfield.name}_{alias_counter}"
                        # Add to select expressions
                        select_expressions.append(f"{sub_full_field} AS `{alias_name}`")
                        # Push subfield to the stack for further processing
                        stack.append((exploded_alias, subfield.name, subfield.dataType))
                else:
                    # For non-struct array elements, sort and coalesce
                    alias_name = f"{full_field.replace('.', '_')}"
                    sort_expr = f"SORT_ARRAY(COALESCE(`{exploded_alias}`, ARRAY())) AS `{alias_name}`"
                    select_expressions.append(sort_expr)
            else:
                # If not exploding, sort and coalesce the array
                alias_name = f"{full_field.replace('.', '_')}"
                sort_expr = f"SORT_ARRAY(COALESCE(`{full_field}`, ARRAY())) AS `{alias_name}`"
                select_expressions.append(sort_expr)

        else:
            # Handle non-nested, non-array fields
            alias_name = f"{full_field.replace('.', '_')}"
            select_expr = f"`{full_field}` AS `{alias_name}`"
            select_expressions.append(select_expr)

    # Join select expressions and lateral view clauses
    select_clause = ",\n    ".join(select_expressions)
    lateral_view_clause = "\n    ".join(lateral_view_clauses)
    sql_query = f"SELECT \n    {select_clause}\nFROM `{table_name}`\n    {lateral_view_clause}"

    return sql_query

# Sample complex nested data
data = [
    (
        1,
        {
            "name": "John Doe",
            "age": 30,
            "contact": {"email": "john@example.com", "phone": "123-456-7890"}
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
        {
            "name": "Alice Smith",
            "age": 28,
            "contact": {"email": "alice@example.com", "phone": "987-654-3210"}
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

# Specify columns to explode (e.g., 'orders', 'preferences')
columns_to_explode = ["orders", "preferences"]

# Generate the flatten SQL query using iterative approach
sql_query = generate_flatten_sql(df, "complex_nested_table", columns_to_explode)
print("Generated SQL Query:\n")
print(sql_query)

# Execute the generated SQL query
flattened_df = spark.sql(sql_query)
flattened_df.show(truncate=False)
flattened_df.printSchema()

# Stop Spark session
spark.stop()
