from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, ArrayType

# Initialize SparkSession
spark = SparkSession.builder \
    .appName("ComplexNestedFlattening") \
    .master("local[*]") \
    .getOrCreate()

def generate_flatten_sql(df, table_name, columns_to_explode=None):
    """
    Generates a SQL query to flatten a nested DataFrame schema iteratively using a stack.

    :param df: Input DataFrame with nested schema
    :param table_name: Name of the table to generate SQL for
    :param columns_to_explode: List of column names to explode in specified order.
    :return: Flattened SQL query as a string
    """
    select_expressions = []
    lateral_view_clauses = []
    alias_counter = 0

    # Stack with controlled order of fields (priority to columns in columns_to_explode list)
    stack = [(None, field.name, field.dataType) for field in df.schema.fields]

    # Set of columns to explode for quick lookup
    explode_columns = set(columns_to_explode or [])

    while stack:
        parent, field, field_type = stack.pop()
        full_field = f"{parent}.{field}" if parent else field

        if isinstance(field_type, StructType):
            # Process StructType fields by pushing subfields onto the stack
            for subfield in field_type.fields:
                sub_full_field = f"{full_field}.{subfield.name}"
                alias_name = f"{full_field.replace('.', '_')}_{subfield.name}"
                select_expressions.append(f"`{sub_full_field}` AS `{alias_name}`")
                stack.append((full_field, subfield.name, subfield.dataType))

        elif isinstance(field_type, ArrayType):
            # Only explode if the column is specified in explode_columns or if explode_columns is empty (explode all)
            should_explode = not explode_columns or full_field in explode_columns
            if should_explode:
                alias_counter += 1
                exploded_alias = f"{field}_exploded_{alias_counter}"

                # Lateral view clause for explosion
                explode_clause = f"LATERAL VIEW EXPLODE(COALESCE(`{full_field}`, ARRAY(NULL))) {exploded_alias} AS `{exploded_alias}`"
                lateral_view_clauses.append(explode_clause)

                element_type = field_type.elementType
                if isinstance(element_type, StructType):
                    # If the array elements are structs, process their subfields
                    for subfield in element_type.fields:
                        sub_full_field = f"{exploded_alias}.{subfield.name}"
                        alias_name = f"{exploded_alias}_{subfield.name}"
                        select_expressions.append(f"`{sub_full_field}` AS `{alias_name}`")
                        stack.append((exploded_alias, subfield.name, subfield.dataType))
                else:
                    # For non-struct arrays, use the exploded alias directly
                    alias_name = f"{full_field.replace('.', '_')}"
                    select_expressions.append(f"`{exploded_alias}` AS `{alias_name}`")
            else:
                # If not exploding, directly use the array
                alias_name = f"{full_field.replace('.', '_')}"
                select_expressions.append(f"`{full_field}` AS `{alias_name}`")

        else:
            # For simple fields, just add them to select_expressions
            alias_name = f"{full_field.replace('.', '_')}"
            select_expressions.append(f"`{full_field}` AS `{alias_name}`")

    # Constructing the SQL query
    select_clause = ",\n    ".join(select_expressions)
    lateral_view_clause = "\n    ".join(lateral_view_clauses)
    sql_query = f"SELECT \n    {select_clause}\nFROM `{table_name}`\n    {lateral_view_clause}"

    return sql_query

# Sample data structure to test the function
data = [
    (
        1,
        {"name": "John Doe", "age": 30, "contact": {"email": "john@example.com", "phone": "123-456-7890"}},
        [
            {
                "order_id": 101,
                "order_items": [{"product": "Laptop", "quantity": 1}],
                "payment_details": {
                    "method": "Credit Card",
                    "billing_address": {"city": "New York"}
                }
            }
        ],
        [{"category": "electronics", "tags": ["tech", "gadgets"]}]
    )
]

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
            StructField("billing_address", StructType([
                StructField("city", StringType(), True)
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

# Specify columns to explode in a specific order
columns_to_explode = ["orders", "orders.order_items", "preferences", "preferences.tags"]

# Generate the flatten SQL query using iterative approach with controlled explosion order
sql_query = generate_flatten_sql(df, "complex_nested_table", columns_to_explode)
print("Generated SQL Query:\n")
print(sql_query)

# Execute the generated SQL query
flattened_df = spark.sql(sql_query)
flattened_df.show(truncate=False)
flattened_df.printSchema()

# Stop Spark session
spark.stop()