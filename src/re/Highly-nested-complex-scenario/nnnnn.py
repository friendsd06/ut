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
    temp_view_name = table_name
    alias_counter = 0

    # Process columns recursively to build SELECT and EXPLODE expressions
    def process_field(parent, field, field_type):
        nonlocal alias_counter
        alias_counter += 1
        if isinstance(field_type, StructType):
            # Handle struct by flattening each field in the struct
            for subfield in field_type.fields:
                field_name = f"{parent}.{field}.{subfield.name}" if parent else f"{field}.{subfield.name}"
                alias_name = f"{parent}_{field}_{subfield.name}" if parent else f"{field}_{subfield.name}"
                select_expressions.append(f"{field_name} AS `{alias_name}`")
                # Recursively process nested structs
                process_field(f"{parent}.{field}" if parent else field, subfield.name, subfield.dataType)
        elif isinstance(field_type, ArrayType):
            # Array of structs or scalars, need to explode
            exploded_alias = f"{field}_exploded_{alias_counter}"
            explode_clause = f"LATERAL VIEW OUTER EXPLODE({parent}.`{field}`) {exploded_alias} AS `{exploded_alias}`" if parent else f"LATERAL VIEW OUTER EXPLODE(`{field}`) {exploded_alias} AS `{exploded_alias}`"
            lateral_view_clauses.append(explode_clause)
            if isinstance(field_type.elementType, StructType):
                # Process each element of the exploded struct array
                process_field(exploded_alias, "", field_type.elementType)
            else:
                # Process scalar array directly
                array_alias = f"{parent}_{field}" if parent else field
                select_expressions.append(f"SORT_ARRAY(COALESCE(COLLECT_LIST(`{exploded_alias}`), ARRAY())) AS `{array_alias}`")
        else:
            # Handle scalar fields by aliasing
            alias_name = f"{parent}_{field}" if parent else field
            select_expressions.append(f"{parent}.`{field}` AS `{alias_name}`" if parent else f"`{field}`")

    # Start processing top-level fields
    for field in df.schema.fields:
        process_field("", field.name, field.dataType)

    # Construct the SQL
    select_clause = ", ".join(select_expressions)
    lateral_view_clause = " ".join(lateral_view_clauses)
    sql_query = f"SELECT {select_clause} FROM `{temp_view_name}` {lateral_view_clause}"

    return sql_query

# Sample complex nested data
data = [
    (1, {"name": "John Doe", "age": 30, "contact": {"email": "john@example.com", "phone": "123-456-7890"}},
     [{"order_id": 101, "order_items": [{"product": "Laptop", "quantity": 1}, {"product": "Mouse", "quantity": 2}],
       "payment_details": {"method": "Credit Card", "card": {"type": "Visa", "number": "****-1234"},
                           "billing_address": {"street": "123 Main St", "city": "New York", "zip": "10001"}}},
      {"order_id": 102, "order_items": [{"product": "Keyboard", "quantity": 1}],
       "payment_details": {"method": "PayPal", "account": "john@paypal.com"}}],
     [{"category": "electronics", "tags": ["tech", "gadgets"]}, {"category": "home", "tags": ["decor", "furniture"]}]
     ),
    (2, {"name": "Alice Smith", "age": 28, "contact": {"email": "alice@example.com", "phone": "987-654-3210"}},
     [{"order_id": 103, "order_items": [{"product": "Desk", "quantity": 1}],
       "payment_details": {"method": "Credit Card", "card": {"type": "MasterCard", "number": "****-5678"},
                           "billing_address": {"street": "456 Elm St", "city": "Los Angeles", "zip": "90001"}}}],
     [{"category": "office", "tags": ["work", "productivity"]}]
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

# Generate and execute the dynamic SQL query to flatten the DataFrame
sql_query = generate_flatten_sql(df, "complex_nested_table")
print("Generated SQL Query:\n", sql_query)

# Execute the generated SQL query
flattened_df = spark.sql(sql_query)
flattened_df.show(truncate=False)
flattened_df.printSchema()

# Stop Spark session
spark.stop()