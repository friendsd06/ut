# Import necessary libraries
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, ArrayType, DoubleType

# Initialize SparkSession
spark = SparkSession.builder \
    .appName("ComplexNestedFlattening") \
    .master("local[*]") \
    .getOrCreate()

# Define a function to generate dynamic SQL for flattening nested structures
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

    def process_struct_field(parent_name, struct_type):
        """Processes struct fields to create SQL aliases for flattening."""
        expressions = []
        for field in struct_type.fields:
            field_name = f"{parent_name}.{field.name}"
            flat_name = f"{parent_name}_{field.name}"
            expressions.append(f"{field_name} AS {flat_name}")
        return expressions

    for field in df.schema.fields:
        if isinstance(field.dataType, StructType):
            # Flatten struct type fields
            select_expressions.extend(process_struct_field(field.name, field.dataType))
        elif isinstance(field.dataType, ArrayType):
            if isinstance(field.dataType.elementType, StructType):
                # Explode and flatten arrays of structs
                select_expressions.append(f"EXPLODE_OUTER({field.name}) AS {field.name}")
            else:
                # Sort scalar arrays and handle nulls
                select_expressions.append(f"SORT_ARRAY(COALESCE({field.name}, ARRAY())) AS {field.name}")
        else:
            # Include non-nested columns as-is
            select_expressions.append(field.name)

    # Join expressions with commas to create the SELECT clause
    select_clause = ", ".join(select_expressions)
    # Construct the final SQL query
    sql_query = f"SELECT {select_clause} FROM {table_name}"

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
