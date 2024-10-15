from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, ArrayType, DoubleType

# Initialize SparkSession
spark = SparkSession.builder \
    .appName("DynamicSQLFlattenNestedColumns") \
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

# Sample data with nested structures for testing
data1 = [
    (1, "John", {"age": 30, "city": "New York"}, [{"type": "home", "number": "123-456-7890"}, {"type": "work", "number": "098-765-4321"}], {"scores": [85, 90, 78], "average": 84.3}),
    (2, "Alice", {"age": 25, "city": "San Francisco"}, [{"type": "home", "number": "111-222-3333"}], {"scores": [92, 88, 95], "average": 91.7}),
    (3, "Bob", {"age": 35, "city": "Chicago"}, [], {"scores": [75, 80, 82], "average": 79.0})
]

schema = StructType([
    StructField("id", IntegerType(), False),
    StructField("name", StringType(), False),
    StructField("info", StructType([StructField("age", IntegerType(), True), StructField("city", StringType(), True)]), True),
    StructField("phones", ArrayType(StructType([StructField("type", StringType(), True), StructField("number", StringType(), True)])), True),
    StructField("grades", StructType([StructField("scores", ArrayType(IntegerType()), True), StructField("average", DoubleType(), True)]), True)
])

# Create DataFrame
df = spark.createDataFrame(data1, schema)

# Register DataFrame as a temporary SQL table
df.createOrReplaceTempView("nested_table")

# Generate and execute the dynamic SQL query
sql_query = generate_flatten_sql(df, "nested_table")
print("Generated SQL Query:\n", sql_query)

# Execute the query and fetch the results
flattened_df = spark.sql(sql_query)
flattened_df.show(truncate=False)
flattened_df.printSchema()

# Stop Spark session
spark.stop()
