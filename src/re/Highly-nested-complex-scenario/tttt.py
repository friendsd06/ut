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
            alias_prefix = f"{field}"  # Use only the field name for aliasing
        else:
            full_field = f"`{field}`"
            alias_prefix = f"{field}"

        if isinstance(field_type, StructType):
            # Handle StructType by processing its subfields
            for subfield in field_type.fields:
                process_field(parent=full_field, field=subfield.name, field_type=subfield.dataType)
        elif isinstance(field_type, ArrayType):
            element_type = field_type.elementType
            if isinstance(element_type, StructType):
                # Handle Array of Structs: Explode and process subfields
                alias_counter += 1
                exploded_alias = f"{field}_exploded_{alias_counter}"

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
                alias_name = f"{field}_sorted"
                # Sort the array to ensure order-independent comparison
                sort_expr = f"SORT_ARRAY({full_field}, TRUE) AS `{alias_name}`"
                select_expressions.append(sort_expr)
        else:
            # Handle Scalar Types: Alias the field with its field name only
            alias_name = f"{field}"
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
