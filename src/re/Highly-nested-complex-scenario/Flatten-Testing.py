from pyspark.sql import DataFrame
from pyspark.sql.types import StructType, ArrayType
from typing import List, Optional

def generate_flatten_sql(schema: StructType, prefix: str = "", separator: str = "_") -> List[str]:
    """
    Recursively generate SQL expressions to flatten a nested schema.

    Args:
    -----
    schema : StructType
        The schema to flatten.
    prefix : str, optional
        Prefix for nested column names.
    separator : str, optional
        Separator for nested column names.

    Returns:
    --------
    List[str]
        List of SQL expressions for flattening.
    """
    expressions = []
    for field in schema.fields:
        column_name = f"{prefix}{field.name}"
        if isinstance(field.dataType, StructType):
            nested_exprs = generate_flatten_sql(field.dataType, f"{column_name}{separator}", separator)
            expressions.extend(nested_exprs)
        elif isinstance(field.dataType, ArrayType):
            if isinstance(field.dataType.elementType, StructType):
                nested_exprs = generate_flatten_sql(field.dataType.elementType, f"{column_name}{separator}", separator)
                array_exprs = [f"explode_outer({column_name}) as {column_name}"]
                array_exprs.extend([f"{expr} as {column_name}{separator}{alias.split(' as ')[1]}" for expr, alias in zip(nested_exprs, nested_exprs)])
                expressions.extend(array_exprs)
            else:
                # For scalar arrays, we keep them as-is
                expressions.append(f"{column_name} as {column_name.replace('.', separator)}")
        else:
            expressions.append(f"{column_name} as {column_name.replace('.', separator)}")
    return expressions

def flatten_delta_table_sql(
        df: DataFrame,
        columns_to_flatten: Optional[List[str]] = None,
        separator: str = "_"
) -> DataFrame:
    """
    Flatten a Delta table using SQL expressions.

    Args:
    -----
    df : DataFrame
        The input Delta table as a Spark DataFrame.
    columns_to_flatten : List[str], optional
        List of column names to flatten. If None, all nested columns will be flattened.
    separator : str, optional
        Separator for nested column names.

    Returns:
    --------
    DataFrame
        A new DataFrame with specified (or all) nested structures flattened.
    """
    schema = df.schema
    flat_expressions = generate_flatten_sql(schema, separator=separator)

    if columns_to_flatten:
        flat_expressions = [expr for expr in flat_expressions if any(expr.startswith(col) for col in columns_to_flatten)]

    # Create a SQL expression that selects all flattened columns
    select_expr = ", ".join(flat_expressions)

    # Create a temporary view of the original DataFrame
    view_name = "temp_view_for_flattening"
    df.createOrReplaceTempView(view_name)

    try:
        # Execute the SQL query using Spark SQL
        flattened_df = spark.sql(f"SELECT {select_expr} FROM {view_name}")
        return flattened_df
    finally:
        # Ensure the temporary view is always dropped
        spark.catalog.dropTempView(view_name)

# Example usage in Databricks notebook
# Assuming 'df' is your input DataFrame

# Test case 1: Flatten all nested columns
df_flat_all = flatten_delta_table_sql(df)
print("\nFlattened DataFrame (all nested columns):")
display(df_flat_all)

# Test case 2: Flatten specific columns
df_flat_specific = flatten_delta_table_sql(df, columns_to_flatten=["info", "phones"])
print("\nFlattened DataFrame (specific columns: info, phones):")
display(df_flat_specific)